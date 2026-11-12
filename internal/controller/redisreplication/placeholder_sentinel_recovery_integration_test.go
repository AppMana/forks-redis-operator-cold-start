package redisreplication

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/redissentinel"
	intctrlutil "github.com/OT-CONTAINER-KIT/redis-operator/internal/controllerutil"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/go-logr/logr/testr"
	rediscli "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestPlaceholderSentinelRecoveryIntegration drives both reconcilers against
// real redis-server and redis-sentinel processes on loopback addresses.
//
// Starting state: two of three Redis pods and two of three Sentinel pods were
// replaced. The surviving Redis is a master holding the dataset with no
// attached replicas. Both replacement Redis pods are empty standalone masters.
// The surviving Sentinel monitors the surviving master and still lists the
// replaced replicas and Sentinels at their old addresses. Both replacement
// Sentinels monitor the unspecified-address placeholder.
//
// The group must converge on the surviving master without that pod ever being
// demoted, restarted or emptied. With a second master that holds data the
// state is ambiguous and both reconcilers must leave every process alone.
//
// Run it with hack/recovery/check-sentinel-placeholder-recovery.sh.
func TestPlaceholderSentinelRecoveryIntegration(t *testing.T) {
	if os.Getenv("REDIS_SENTINEL_RECOVERY_TEST") != "1" {
		t.Skip("set REDIS_SENTINEL_RECOVERY_TEST=1 to run against real redis-server and redis-sentinel processes")
	}
	t.Run("empty replacements rejoin the surviving data master", func(t *testing.T) {
		runPlaceholderSentinelRecovery(t, false)
	})
	t.Run("a second master holding data keeps both controllers refusing", func(t *testing.T) {
		runPlaceholderSentinelRecovery(t, true)
	})
}

func runPlaceholderSentinelRecovery(t *testing.T, secondDataMaster bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// Show the reconcilers' own decisions in the test output.
	ctx = ctrl.LoggerInto(ctx, testr.New(t))

	const (
		ns          = "default"
		rrName      = "example-replication"
		rsName      = "example"
		masterGroup = "mymaster"

		survivorIP    = "127.0.1.10"
		replacement1  = "127.0.1.11"
		replacement2  = "127.0.1.12"
		replaced1     = "127.0.3.11"
		replaced2     = "127.0.3.12"
		sentinel0IP   = "127.0.2.10"
		sentinel1IP   = "127.0.2.11"
		sentinel2IP   = "127.0.2.12"
		oldSentinel1  = "127.0.4.11"
		oldSentinel2  = "127.0.4.12"
		datasetKeys   = 200
		downAfterMS   = "2000"
		failoverMS    = "10000"
		sentinelCount = 3
	)

	startRedis := func(ip, replicaOf string) *exec.Cmd {
		t.Helper()
		dir := t.TempDir()
		conf := fmt.Sprintf("bind %s\nbind-source-addr %s\nreplica-announce-ip %s\nport 6379\nprotected-mode no\nsave \"\"\nappendonly no\ndir %s\n", ip, ip, ip, dir)
		if replicaOf != "" {
			conf += fmt.Sprintf("replicaof %s 6379\n", replicaOf)
		}
		return startProcess(t, dir, "redis-server", conf)
	}
	startSentinel := func(ip, monitor string) *exec.Cmd {
		t.Helper()
		dir := t.TempDir()
		conf := fmt.Sprintf("bind %s\nbind-source-addr %s\nport 26379\nprotected-mode no\ndir %s\n"+
			"sentinel announce-ip %s\nsentinel monitor %s %s 6379 2\n"+
			"sentinel down-after-milliseconds %s %s\nsentinel failover-timeout %s %s\nsentinel parallel-syncs %s 1\n",
			ip, ip, dir, ip, masterGroup, monitor, masterGroup, downAfterMS, masterGroup, failoverMS, masterGroup)
		return startProcess(t, dir, "redis-sentinel", conf)
	}
	redisClient := func(ip, port string) *rediscli.Client {
		return rediscli.NewClient(&rediscli.Options{Addr: ip + ":" + port, DialTimeout: 2 * time.Second})
	}
	type redisState struct {
		info  k8sutils.ReplicationNodeInfo
		keys  int64
		runID string
	}
	observeRedis := func(ip string) (redisState, error) {
		c := redisClient(ip, "6379")
		defer c.Close()
		raw, err := c.Info(ctx, "replication").Result()
		if err != nil {
			return redisState{}, err
		}
		info, err := k8sutils.ParseReplicationNodeInfo(raw)
		if err != nil {
			return redisState{}, err
		}
		keys, err := c.DBSize(ctx).Result()
		if err != nil {
			return redisState{}, err
		}
		server, err := c.Info(ctx, "server").Result()
		if err != nil {
			return redisState{}, err
		}
		return redisState{info: info, keys: keys, runID: infoField(server, "run_id")}, nil
	}
	type sentinelState struct {
		host, flags, status string
		slaves, sentinels   int
		peers               map[string]string
		quorumOK            bool
	}
	observeSentinel := func(ip string) (sentinelState, error) {
		c := redisClient(ip, "26379")
		defer c.Close()
		masterCmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", masterGroup)
		if err := c.Process(ctx, masterCmd); err != nil {
			return sentinelState{}, err
		}
		fields := masterCmd.Val()
		state := sentinelState{host: fields["ip"], flags: fields["flags"], peers: map[string]string{}}
		_, _ = fmt.Sscan(fields["num-slaves"], &state.slaves)
		_, _ = fmt.Sscan(fields["num-other-sentinels"], &state.sentinels)
		state.sentinels++
		raw, err := c.Info(ctx, "sentinel").Result()
		if err != nil {
			return sentinelState{}, err
		}
		for _, line := range strings.Split(raw, "\n") {
			if strings.HasPrefix(line, "master0:") {
				for _, field := range strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "master0:")), ",") {
					if value, ok := strings.CutPrefix(field, "status="); ok {
						state.status = value
					}
				}
			}
		}
		peerCmd := rediscli.NewMapStringStringSliceCmd(ctx, "SENTINEL", "SENTINELS", masterGroup)
		if err := c.Process(ctx, peerCmd); err != nil {
			return sentinelState{}, err
		}
		for _, peer := range peerCmd.Val() {
			state.peers[peer["ip"]] = peer["flags"]
		}
		quorum, err := c.Do(ctx, "SENTINEL", "CKQUORUM", masterGroup).Text()
		state.quorumOK = err == nil && strings.HasPrefix(quorum, "OK ")
		return state, nil
	}

	// 1. A healthy group: the survivor is the master of two replicas, watched
	// by three Sentinels.
	startRedis(survivorIP, "")
	waitFor(t, ctx, "survivor answers", func() bool { _, err := observeRedis(survivorIP); return err == nil })
	oldReplica1 := startRedis(replaced1, survivorIP)
	oldReplica2 := startRedis(replaced2, survivorIP)
	writer := redisClient(survivorIP, "6379")
	defer writer.Close()
	for i := 0; i < datasetKeys; i++ {
		require.NoError(t, writer.Set(ctx, fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i), 0).Err())
	}
	waitFor(t, ctx, "both original replicas hold the dataset", func() bool {
		for _, ip := range []string{replaced1, replaced2} {
			state, err := observeRedis(ip)
			if err != nil || state.info.MasterLinkStatus != "up" || state.keys != datasetKeys {
				return false
			}
		}
		return true
	})
	startSentinel(sentinel0IP, survivorIP)
	oldSentinelProc1 := startSentinel(oldSentinel1, survivorIP)
	oldSentinelProc2 := startSentinel(oldSentinel2, survivorIP)
	waitFor(t, ctx, "three Sentinels know the master, its replicas and each other", func() bool {
		for _, ip := range []string{sentinel0IP, oldSentinel1, oldSentinel2} {
			state, err := observeSentinel(ip)
			if err != nil || state.host != survivorIP || state.slaves != 2 || state.sentinels != sentinelCount || !state.quorumOK {
				return false
			}
		}
		return true
	})

	// 2. Two Redis pods and two Sentinel pods are replaced: the old processes
	// die and their replacements come up empty at new addresses.
	for _, proc := range []*exec.Cmd{oldReplica1, oldReplica2, oldSentinelProc1, oldSentinelProc2} {
		require.NoError(t, proc.Process.Kill())
		_, _ = proc.Process.Wait()
	}
	startRedis(replacement1, "")
	startRedis(replacement2, "")
	startSentinel(sentinel1IP, "0.0.0.0")
	startSentinel(sentinel2IP, "0.0.0.0")

	var before redisState
	waitFor(t, ctx, "the observed failure state", func() bool {
		var err error
		before, err = observeRedis(survivorIP)
		if err != nil || before.info.Role != "master" || before.info.ConnectedSlaves != 0 {
			return false
		}
		for _, ip := range []string{replacement1, replacement2} {
			state, err := observeRedis(ip)
			if err != nil || state.info.Role != "master" || state.keys != 0 || state.info.EffectiveOffset() != 0 || state.info.ConnectedSlaves != 0 {
				return false
			}
		}
		for _, ip := range []string{sentinel1IP, sentinel2IP} {
			state, err := observeSentinel(ip)
			if err != nil || state.host != "0.0.0.0" || !strings.Contains(state.flags, "s_down") || state.sentinels != 1 {
				return false
			}
		}
		state, err := observeSentinel(sentinel0IP)
		if err != nil || state.host != survivorIP || state.status != "ok" || state.slaves != 2 {
			return false
		}
		return strings.Contains(state.peers[oldSentinel1], "s_down") && strings.Contains(state.peers[oldSentinel2], "s_down")
	})
	require.EqualValues(t, datasetKeys, before.keys)
	require.Positive(t, before.info.EffectiveOffset())
	require.NotEmpty(t, before.runID)
	t.Logf("failure state: survivor role=%s keys=%d offset=%d replid=%s connected_slaves=%d; replacements are empty masters; two of three Sentinels monitor 0.0.0.0",
		before.info.Role, before.keys, before.info.EffectiveOffset(), before.info.ReplID, before.info.ConnectedSlaves)

	if secondDataMaster {
		stray := redisClient(replacement1, "6379")
		require.NoError(t, stray.Set(ctx, "stray", "write", 0).Err())
		require.NoError(t, stray.Close())
	}

	// 3. The Kubernetes view of that state.
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	require.NoError(t, rsvb2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	rr := &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: rrName, Namespace: ns, UID: "rr-uid", Finalizers: []string{RedisReplicationFinalizer}},
		Spec: rrvb2.RedisReplicationSpec{
			Size:             ptr.To(int32(3)),
			KubernetesConfig: commonapi.KubernetesConfig{Image: "redis:7"},
		},
		// The recorded master is one of the replaced pods.
		Status: rrvb2.RedisReplicationStatus{MasterNode: rrName + "-2"},
	}
	rs := &rsvb2.RedisSentinel{
		ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: ns, UID: "rs-uid", Finalizers: []string{redissentinel.RedisSentinelFinalizer}},
		Spec: rsvb2.RedisSentinelSpec{
			Size:             ptr.To(int32(sentinelCount)),
			KubernetesConfig: commonapi.KubernetesConfig{Image: "redis-sentinel:7"},
			RedisSentinelConfig: &rsvb2.RedisSentinelConfig{
				RedisSentinelConfig: commonapi.RedisSentinelConfig{
					RedisReplicationName: rrName,
					MasterGroupName:      masterGroup,
					SentinelConfig: commonapi.SentinelConfig{
						Quorum:                "2",
						DownAfterMilliseconds: downAfterMS,
						FailoverTimeout:       failoverMS,
						ParallelSyncs:         "1",
					},
				},
			},
		},
	}
	redisLabels := common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil)
	sentinelLabels := common.GetRedisLabels(rs.GetStatefulSetName(), common.SetupTypeSentinel, "sentinel", nil)
	statefulSet := func(name string, labels map[string]string) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3)), Selector: &metav1.LabelSelector{MatchLabels: labels}},
			Status:     appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3},
		}
	}
	redisSTS := statefulSet(rrName, redisLabels)
	sentinelSTS := statefulSet(rs.GetStatefulSetName(), sentinelLabels)
	survivorLabels := map[string]string{common.RedisRoleLabelKey: common.RedisRoleLabelSlave}
	for k, v := range redisLabels {
		survivorLabels[k] = v
	}
	pods := []*corev1.Pod{
		newRunningPodForTest(ns, rrName+"-0", survivorIP, survivorLabels, true),
		newRunningPodForTest(ns, rrName+"-1", replacement1, redisLabels, true),
		newRunningPodForTest(ns, rrName+"-2", replacement2, redisLabels, true),
		newRunningPodForTest(ns, rs.GetStatefulSetName()+"-0", sentinel0IP, sentinelLabels, true),
		newRunningPodForTest(ns, rs.GetStatefulSetName()+"-1", sentinel1IP, sentinelLabels, true),
		newRunningPodForTest(ns, rs.GetStatefulSetName()+"-2", sentinel2IP, sentinelLabels, true),
	}
	k8sObjects := []runtime.Object{redisSTS, sentinelSTS}
	ctrlBuilder := clientfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&rrvb2.RedisReplication{}, &rsvb2.RedisSentinel{}).
		WithObjects(rr, rs, redisSTS, sentinelSTS)
	for _, pod := range pods {
		k8sObjects = append(k8sObjects, pod)
		ctrlBuilder = ctrlBuilder.WithObjects(pod)
	}
	k8sClient := k8sfake.NewSimpleClientset(k8sObjects...)
	ctrlClient := ctrlBuilder.Build()
	healer := redishealer.NewHealer(k8sClient)
	checker := redishealer.NewChecker(k8sClient)
	rrReconciler := &Reconciler{Client: ctrlClient, StatefulSet: &fakeStatefulSetService{}, Healer: healer, Checker: checker, K8sClient: k8sClient}
	rsReconciler := &redissentinel.RedisSentinelReconciler{
		Client: ctrlClient, Checker: checker, Healer: healer, K8sClient: k8sClient,
		ReplicationWatcher: intctrlutil.NewResourceWatcher(),
	}

	// 4. Reconcile as the manager would: a refusal is a requeue, not a test
	// failure. The dataset must be intact on the survivor after every step.
	requireSurvivorIntact := func(step string) {
		t.Helper()
		state, err := observeRedis(survivorIP)
		require.NoError(t, err, step)
		require.Equal(t, "master", state.info.Role, "%s: the data-holding pod must never be demoted", step)
		require.EqualValues(t, datasetKeys, state.keys, "%s: the dataset must never shrink", step)
		require.Equal(t, before.runID, state.runID, "%s: the data-holding pod must not restart", step)
		require.Equal(t, before.info.ReplID, state.info.ReplID, "%s: the data-holding pod must keep its replication history", step)
	}
	converged := func() (bool, string) {
		survivorState, err := observeRedis(survivorIP)
		if err != nil || survivorState.info.Role != "master" || survivorState.info.ConnectedSlaves != 2 {
			return false, fmt.Sprintf("survivor role=%s connected_slaves=%d err=%v", survivorState.info.Role, survivorState.info.ConnectedSlaves, err)
		}
		for _, ip := range []string{replacement1, replacement2} {
			state, err := observeRedis(ip)
			if err != nil || state.info.Role != "slave" || state.info.MasterHost != survivorIP || state.info.MasterLinkStatus != "up" || state.keys != datasetKeys {
				return false, fmt.Sprintf("replacement %s role=%s master_host=%s link=%s keys=%d err=%v", ip, state.info.Role, state.info.MasterHost, state.info.MasterLinkStatus, state.keys, err)
			}
		}
		for _, ip := range []string{sentinel0IP, sentinel1IP, sentinel2IP} {
			state, err := observeSentinel(ip)
			if err != nil || state.host != survivorIP || state.status != "ok" || state.slaves != 2 || state.sentinels != sentinelCount || !state.quorumOK || len(state.peers) != sentinelCount-1 {
				return false, fmt.Sprintf("sentinel %s monitors=%s status=%s slaves=%d sentinels=%d quorumOK=%v peers=%v err=%v", ip, state.host, state.status, state.slaves, state.sentinels, state.quorumOK, state.peers, err)
			}
			for peer, flags := range state.peers {
				if peer == oldSentinel1 || peer == oldSentinel2 || strings.Contains(flags, "s_down") || strings.Contains(flags, "disconnected") {
					return false, fmt.Sprintf("sentinel %s retains peer %s flags=%s", ip, peer, flags)
				}
			}
		}
		current := &rrvb2.RedisReplication{}
		if err := ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, current); err != nil || current.Status.MasterNode != rrName+"-0" {
			return false, fmt.Sprintf("status.masterNode=%s err=%v", current.Status.MasterNode, err)
		}
		for name, want := range map[string]string{rrName + "-0": common.RedisRoleLabelMaster, rrName + "-1": common.RedisRoleLabelSlave, rrName + "-2": common.RedisRoleLabelSlave} {
			pod, err := k8sClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil || pod.Labels[common.RedisRoleLabelKey] != want {
				return false, fmt.Sprintf("pod %s role label=%q want %q err=%v", name, pod.Labels[common.RedisRoleLabelKey], want, err)
			}
		}
		return true, ""
	}

	passes := 12
	if secondDataMaster {
		passes = 4
	}
	reason := ""
	done := false
	for pass := 1; pass <= passes && !done; pass++ {
		current := &rrvb2.RedisReplication{}
		require.NoError(t, ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, current))
		if _, err := rrReconciler.reconcileStatus(ctx, current); err != nil {
			t.Logf("pass %d: RedisReplication status: %v", pass, err)
		}
		requireSurvivorIntact(fmt.Sprintf("pass %d after RedisReplication status", pass))

		require.NoError(t, ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, current))
		if result, err := rrReconciler.reconcileRedis(ctx, current); err != nil {
			t.Logf("pass %d: RedisReplication redis: %v", pass, err)
		} else {
			t.Logf("pass %d: RedisReplication redis requeueAfter=%s", pass, result.RequeueAfter)
		}
		requireSurvivorIntact(fmt.Sprintf("pass %d after RedisReplication redis", pass))

		if _, err := rsReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: rsName}}); err != nil {
			t.Logf("pass %d: RedisSentinel: %v", pass, err)
		}
		requireSurvivorIntact(fmt.Sprintf("pass %d after RedisSentinel", pass))

		done, reason = converged()
		if !done {
			t.Logf("pass %d: not converged: %s", pass, reason)
			time.Sleep(time.Second)
		}
	}
	if secondDataMaster {
		require.False(t, done)
		for _, ip := range []string{replacement1, replacement2} {
			state, err := observeRedis(ip)
			require.NoError(t, err)
			require.Equal(t, "master", state.info.Role, "%s must not be reattached while two masters hold data", ip)
		}
		stray := redisClient(replacement1, "6379")
		value, err := stray.Get(ctx, "stray").Result()
		require.NoError(t, stray.Close())
		require.NoError(t, err)
		require.Equal(t, "write", value, "the second master's data must not be discarded")
		for _, ip := range []string{sentinel1IP, sentinel2IP} {
			state, err := observeSentinel(ip)
			require.NoError(t, err)
			require.Equal(t, "0.0.0.0", state.host, "Sentinel %s must not be pointed at either master", ip)
		}
		state, err := observeSentinel(sentinel0IP)
		require.NoError(t, err)
		require.Equal(t, survivorIP, state.host, "the configured Sentinel must keep its monitor")
		for _, pod := range pods[:3] {
			live, err := k8sClient.CoreV1().Pods(ns).Get(ctx, pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotEqual(t, common.RedisRoleLabelMaster, live.Labels[common.RedisRoleLabelKey], "no pod may enter the writable Service")
		}
		return
	}
	require.True(t, done, "group did not converge on the data-holding master after %d reconcile passes: %s", passes, reason)

	// 5. Every key written before the failure is served by all three pods.
	for _, ip := range []string{survivorIP, replacement1, replacement2} {
		c := redisClient(ip, "6379")
		for i := 0; i < datasetKeys; i++ {
			value, err := c.Get(ctx, fmt.Sprintf("key:%d", i)).Result()
			require.NoError(t, err, "%s key:%d", ip, i)
			require.Equal(t, fmt.Sprintf("value:%d", i), value, "%s key:%d", ip, i)
		}
		require.NoError(t, c.Close())
	}
	requireSurvivorIntact("after convergence")
}

func startProcess(t *testing.T, dir, binary, conf string) *exec.Cmd {
	t.Helper()
	path := filepath.Join(dir, binary+".conf")
	require.NoError(t, os.WriteFile(path, []byte(conf), 0o600))
	logFile, err := os.Create(filepath.Join(dir, binary+".log"))
	require.NoError(t, err)
	cmd := exec.CommandContext(context.Background(), binary, path)
	cmd.Dir = dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start(), "start %s", binary)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
		if t.Failed() {
			if raw, err := os.ReadFile(logFile.Name()); err == nil {
				lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
				if len(lines) > 25 {
					lines = lines[len(lines)-25:]
				}
				t.Logf("%s (%s):\n%s", binary, strings.SplitN(conf, "\n", 2)[0], strings.Join(lines, "\n"))
			}
		}
	})
	return cmd
}

func waitFor(t *testing.T, ctx context.Context, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if condition() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func infoField(info, field string) string {
	for _, line := range strings.Split(strings.ReplaceAll(info, "\r\n", "\n"), "\n") {
		if value, ok := strings.CutPrefix(line, field+":"); ok {
			return value
		}
	}
	return ""
}
