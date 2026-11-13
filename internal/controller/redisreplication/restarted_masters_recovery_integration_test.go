package redisreplication

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	labNamespace   = "default"
	labReplication = "example-replication"
	labSentinel    = "example"
	labMasterGroup = "mymaster"
	labDownAfterMS = "2000"
	labFailoverMS  = "10000"
)

// labStartRedis starts a redis-server bound to ip. With dataDir it persists an
// append-only file there, so a later process started on the same directory
// loads the dataset the way a restarted pod with a volume does.
func labStartRedis(t *testing.T, ip, replicaOf, dataDir string) *exec.Cmd {
	t.Helper()
	persistence := "save \"\"\nappendonly no\n"
	if dataDir == "" {
		dataDir = t.TempDir()
	} else {
		persistence = "save \"\"\nappendonly yes\nappendfsync always\n"
	}
	conf := fmt.Sprintf("bind %s\nbind-source-addr %s\nreplica-announce-ip %s\nport 6379\nprotected-mode no\n%sdir %s\n", ip, ip, ip, persistence, dataDir)
	if replicaOf != "" {
		conf += fmt.Sprintf("replicaof %s 6379\n", replicaOf)
	}
	return startProcess(t, dataDir, "redis-server", conf)
}

func labStartSentinel(t *testing.T, ip, monitor string) *exec.Cmd {
	t.Helper()
	dir := t.TempDir()
	conf := fmt.Sprintf("bind %s\nbind-source-addr %s\nport 26379\nprotected-mode no\ndir %s\n"+
		"sentinel announce-ip %s\nsentinel monitor %s %s 6379 2\n"+
		"sentinel down-after-milliseconds %s %s\nsentinel failover-timeout %s %s\nsentinel parallel-syncs %s 1\n",
		ip, ip, dir, ip, labMasterGroup, monitor, labMasterGroup, labDownAfterMS, labMasterGroup, labFailoverMS, labMasterGroup)
	return startProcess(t, dir, "redis-sentinel", conf)
}

func labRedisClient(ip, port string) *rediscli.Client {
	return rediscli.NewClient(&rediscli.Options{Addr: ip + ":" + port, DialTimeout: 2 * time.Second})
}

type labRedisState struct {
	info  k8sutils.ReplicationNodeInfo
	keys  int64
	runID string
}

func labObserveRedis(ctx context.Context, ip string) (labRedisState, error) {
	c := labRedisClient(ip, "6379")
	defer c.Close()
	raw, err := c.Info(ctx, "replication").Result()
	if err != nil {
		return labRedisState{}, err
	}
	info, err := k8sutils.ParseReplicationNodeInfo(raw)
	if err != nil {
		return labRedisState{}, err
	}
	keys, err := c.DBSize(ctx).Result()
	if err != nil {
		return labRedisState{}, err
	}
	server, err := c.Info(ctx, "server").Result()
	if err != nil {
		return labRedisState{}, err
	}
	return labRedisState{info: info, keys: keys, runID: infoField(server, "run_id")}, nil
}

type labSentinelState struct {
	host, flags, status string
	slaves, sentinels   int
	peers               map[string]string
	quorumOK            bool
}

func labObserveSentinel(ctx context.Context, ip string) (labSentinelState, error) {
	c := labRedisClient(ip, "26379")
	defer c.Close()
	masterCmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", labMasterGroup)
	if err := c.Process(ctx, masterCmd); err != nil {
		return labSentinelState{}, err
	}
	fields := masterCmd.Val()
	state := labSentinelState{host: fields["ip"], flags: fields["flags"], peers: map[string]string{}}
	_, _ = fmt.Sscan(fields["num-slaves"], &state.slaves)
	_, _ = fmt.Sscan(fields["num-other-sentinels"], &state.sentinels)
	state.sentinels++
	raw, err := c.Info(ctx, "sentinel").Result()
	if err != nil {
		return labSentinelState{}, err
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
	peerCmd := rediscli.NewMapStringStringSliceCmd(ctx, "SENTINEL", "SENTINELS", labMasterGroup)
	if err := c.Process(ctx, peerCmd); err != nil {
		return labSentinelState{}, err
	}
	for _, peer := range peerCmd.Val() {
		state.peers[peer["ip"]] = peer["flags"]
	}
	quorum, err := c.Do(ctx, "SENTINEL", "CKQUORUM", labMasterGroup).Text()
	state.quorumOK = err == nil && strings.HasPrefix(quorum, "OK ")
	return state, nil
}

// labControllers is the Kubernetes view of a three-pod replication watched by
// a three-member RedisSentinel, with both reconcilers wired to live probes.
type labControllers struct {
	ctrlClient   client.Client
	k8sClient    kubernetes.Interface
	replication  *Reconciler
	sentinel     *redissentinel.RedisSentinelReconciler
	redisPodName [3]string
}

func newLabControllers(t *testing.T, recordedMaster string, redisIPs, sentinelIPs [3]string, roleLabels map[string]string) *labControllers {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	require.NoError(t, rsvb2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	rr := &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: labReplication, Namespace: labNamespace, UID: "rr-uid", Finalizers: []string{RedisReplicationFinalizer}},
		Spec: rrvb2.RedisReplicationSpec{
			Size:             ptr.To(int32(3)),
			KubernetesConfig: commonapi.KubernetesConfig{Image: "redis:7"},
		},
		Status: rrvb2.RedisReplicationStatus{MasterNode: recordedMaster},
	}
	rs := &rsvb2.RedisSentinel{
		ObjectMeta: metav1.ObjectMeta{Name: labSentinel, Namespace: labNamespace, UID: "rs-uid", Finalizers: []string{redissentinel.RedisSentinelFinalizer}},
		Spec: rsvb2.RedisSentinelSpec{
			Size:             ptr.To(int32(3)),
			KubernetesConfig: commonapi.KubernetesConfig{Image: "redis-sentinel:7"},
			RedisSentinelConfig: &rsvb2.RedisSentinelConfig{
				RedisSentinelConfig: commonapi.RedisSentinelConfig{
					RedisReplicationName: labReplication,
					MasterGroupName:      labMasterGroup,
					SentinelConfig: commonapi.SentinelConfig{
						Quorum:                "2",
						DownAfterMilliseconds: labDownAfterMS,
						FailoverTimeout:       labFailoverMS,
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
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: labNamespace},
			Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3)), Selector: &metav1.LabelSelector{MatchLabels: labels}},
			Status:     appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3},
		}
	}
	redisSTS := statefulSet(labReplication, redisLabels)
	sentinelSTS := statefulSet(rs.GetStatefulSetName(), sentinelLabels)
	lab := &labControllers{}
	k8sObjects := []runtime.Object{redisSTS, sentinelSTS}
	ctrlBuilder := clientfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&rrvb2.RedisReplication{}, &rsvb2.RedisSentinel{}).
		WithObjects(rr, rs, redisSTS, sentinelSTS)
	for i := 0; i < 3; i++ {
		lab.redisPodName[i] = fmt.Sprintf("%s-%d", labReplication, i)
		labels := map[string]string{}
		for k, v := range redisLabels {
			labels[k] = v
		}
		if role := roleLabels[lab.redisPodName[i]]; role != "" {
			labels[common.RedisRoleLabelKey] = role
		}
		for _, pod := range []*corev1.Pod{
			newRunningPodForTest(labNamespace, lab.redisPodName[i], redisIPs[i], labels, true),
			newRunningPodForTest(labNamespace, fmt.Sprintf("%s-%d", rs.GetStatefulSetName(), i), sentinelIPs[i], sentinelLabels, true),
		} {
			k8sObjects = append(k8sObjects, pod)
			ctrlBuilder = ctrlBuilder.WithObjects(pod)
		}
	}
	lab.k8sClient = k8sfake.NewSimpleClientset(k8sObjects...)
	lab.ctrlClient = ctrlBuilder.Build()
	healer := redishealer.NewHealer(lab.k8sClient)
	checker := redishealer.NewChecker(lab.k8sClient)
	lab.replication = &Reconciler{Client: lab.ctrlClient, StatefulSet: &fakeStatefulSetService{}, Healer: healer, Checker: checker, K8sClient: lab.k8sClient}
	lab.sentinel = &redissentinel.RedisSentinelReconciler{
		Client: lab.ctrlClient, Checker: checker, Healer: healer, K8sClient: lab.k8sClient,
		ReplicationWatcher: intctrlutil.NewResourceWatcher(),
	}
	return lab
}

func (l *labControllers) recordedMaster(ctx context.Context, t *testing.T) string {
	t.Helper()
	current := &rrvb2.RedisReplication{}
	require.NoError(t, l.ctrlClient.Get(ctx, types.NamespacedName{Namespace: labNamespace, Name: labReplication}, current))
	return current.Status.MasterNode
}

func (l *labControllers) roleLabel(ctx context.Context, t *testing.T, pod string) string {
	t.Helper()
	live, err := l.k8sClient.CoreV1().Pods(labNamespace).Get(ctx, pod, metav1.GetOptions{})
	require.NoError(t, err)
	return live.Labels[common.RedisRoleLabelKey]
}

// reconcileOnce runs one pass of both controllers the way the manager would:
// a refusal is a requeue, not a test failure. afterStep runs after every step.
func (l *labControllers) reconcileOnce(ctx context.Context, t *testing.T, pass int, afterStep func(step string)) {
	t.Helper()
	key := types.NamespacedName{Namespace: labNamespace, Name: labReplication}
	current := &rrvb2.RedisReplication{}
	require.NoError(t, l.ctrlClient.Get(ctx, key, current))
	if _, err := l.replication.reconcileStatus(ctx, current); err != nil {
		t.Logf("pass %d: RedisReplication status: %v", pass, err)
	}
	afterStep(fmt.Sprintf("pass %d after RedisReplication status", pass))

	require.NoError(t, l.ctrlClient.Get(ctx, key, current))
	if result, err := l.replication.reconcileRedis(ctx, current); err != nil {
		t.Logf("pass %d: RedisReplication redis: %v", pass, err)
	} else {
		t.Logf("pass %d: RedisReplication redis requeueAfter=%s", pass, result.RequeueAfter)
	}
	afterStep(fmt.Sprintf("pass %d after RedisReplication redis", pass))

	if _, err := l.sentinel.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: labNamespace, Name: labSentinel}}); err != nil {
		t.Logf("pass %d: RedisSentinel: %v", pass, err)
	}
	afterStep(fmt.Sprintf("pass %d after RedisSentinel", pass))
}

// TestRestartedMastersRecoveryIntegration drives both reconcilers against real
// redis-server and redis-sentinel processes on loopback addresses.
//
// Starting state: every Redis pod of a persistent replication restarted from
// its own append-only file at a new address, and every Sentinel was replaced.
// All three Redis pods are masters holding data, with no attached replicas and
// a replication ID of their own. The recorded master holds the newest write;
// another pod holds more keys. No Sentinel knows another Sentinel.
//
// The group must converge on the recorded master, without that pod being
// demoted or restarted, and every Sentinel must monitor it. A Sentinel that
// names another live pod is an election the record cannot overrule: then both
// reconcilers must leave every process alone.
//
// Run it with hack/recovery/check-sentinel-placeholder-recovery.sh.
func TestRestartedMastersRecoveryIntegration(t *testing.T) {
	if os.Getenv("REDIS_SENTINEL_RECOVERY_TEST") != "1" {
		t.Skip("set REDIS_SENTINEL_RECOVERY_TEST=1 to run against real redis-server and redis-sentinel processes")
	}
	const (
		placeholder = "0.0.0.0"
		oldMasterIP = "127.0.3.10"
		liveOtherIP = "127.0.1.11"
	)
	for _, tc := range []struct {
		name        string
		monitors    [3]string
		wantRefusal bool
	}{
		{name: "every Sentinel on the placeholder", monitors: [3]string{placeholder, placeholder, placeholder}},
		{name: "one Sentinel on the dead address of the recorded master", monitors: [3]string{oldMasterIP, placeholder, placeholder}},
		{name: "a Sentinel names another live pod", monitors: [3]string{placeholder, liveOtherIP, placeholder}, wantRefusal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runRestartedMastersRecovery(t, tc.monitors, tc.wantRefusal)
		})
	}
}

func runRestartedMastersRecovery(t *testing.T, monitors [3]string, wantRefusal bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	ctx = ctrl.LoggerInto(ctx, testr.New(t))

	const (
		datasetKeys = 200
		strayKeys   = 5
	)
	oldIPs := [3]string{"127.0.3.10", "127.0.3.11", "127.0.3.12"}
	redisIPs := [3]string{"127.0.1.10", "127.0.1.11", "127.0.1.12"}
	sentinelIPs := [3]string{"127.0.2.10", "127.0.2.11", "127.0.2.12"}
	dataDirs := [3]string{t.TempDir(), t.TempDir(), t.TempDir()}

	// 1. A healthy persistent group: pod 0 is the master of two replicas.
	procs := [3]*exec.Cmd{labStartRedis(t, oldIPs[0], "", dataDirs[0])}
	waitFor(t, ctx, "the master answers", func() bool { _, err := labObserveRedis(ctx, oldIPs[0]); return err == nil })
	procs[1] = labStartRedis(t, oldIPs[1], oldIPs[0], dataDirs[1])
	procs[2] = labStartRedis(t, oldIPs[2], oldIPs[0], dataDirs[2])
	writer := labRedisClient(oldIPs[0], "6379")
	for i := 0; i < datasetKeys; i++ {
		require.NoError(t, writer.Set(ctx, fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i), 0).Err())
	}
	waitFor(t, ctx, "both replicas hold the dataset", func() bool {
		for _, ip := range oldIPs[1:] {
			state, err := labObserveRedis(ctx, ip)
			if err != nil || state.info.MasterLinkStatus != "up" || state.keys != datasetKeys {
				return false
			}
		}
		return true
	})

	// 2. The replicas stop first, the master takes one more write, then it
	// stops too. Every pod restarts from its own disk at a new address.
	stop := func(proc *exec.Cmd) {
		require.NoError(t, proc.Process.Kill())
		_, _ = proc.Process.Wait()
	}
	stop(procs[1])
	stop(procs[2])
	require.NoError(t, writer.Set(ctx, "latest", "write", 0).Err())
	require.NoError(t, writer.Close())
	stop(procs[0])
	for i := range redisIPs {
		labStartRedis(t, redisIPs[i], "", dataDirs[i])
	}
	for i := range sentinelIPs {
		labStartSentinel(t, sentinelIPs[i], monitors[i])
	}
	// A restarted former replica holds more keys than the recorded master,
	// which does not make its copy the newer one.
	waitFor(t, ctx, "the restarted former replica answers", func() bool { _, err := labObserveRedis(ctx, redisIPs[1]); return err == nil })
	stray := labRedisClient(redisIPs[1], "6379")
	for i := 0; i < strayKeys; i++ {
		require.NoError(t, stray.Set(ctx, fmt.Sprintf("stray:%d", i), "write", 0).Err())
	}
	require.NoError(t, stray.Close())

	var before [3]labRedisState
	waitFor(t, ctx, "the observed failure state", func() bool {
		for i, ip := range redisIPs {
			state, err := labObserveRedis(ctx, ip)
			if err != nil || state.info.Role != "master" || state.info.ConnectedSlaves != 0 {
				return false
			}
			before[i] = state
		}
		for i, ip := range sentinelIPs {
			state, err := labObserveSentinel(ctx, ip)
			if err != nil || state.host != monitors[i] || state.sentinels != 1 {
				return false
			}
			if monitors[i] != redisIPs[1] && !strings.Contains(state.flags, "s_down") {
				return false
			}
		}
		return true
	})
	require.EqualValues(t, datasetKeys+1, before[0].keys)
	require.EqualValues(t, datasetKeys+strayKeys, before[1].keys)
	require.EqualValues(t, datasetKeys, before[2].keys)
	require.NotEqual(t, before[0].info.ReplID, before[1].info.ReplID)
	require.NotEqual(t, before[0].info.ReplID, before[2].info.ReplID)
	require.NotEqual(t, before[1].info.ReplID, before[2].info.ReplID)
	for i, state := range before {
		t.Logf("failure state: pod %d role=%s keys=%d offset=%d replid=%s connected_slaves=%d; Sentinel %d monitors %s",
			i, state.info.Role, state.keys, state.info.EffectiveOffset(), state.info.ReplID, state.info.ConnectedSlaves, i, monitors[i])
	}

	// 3. The Kubernetes view of that state: pod 0 is the recorded master.
	lab := newLabControllers(t, labReplication+"-0", redisIPs, sentinelIPs, nil)

	requireRecordedMasterIntact := func(step string) {
		t.Helper()
		state, err := labObserveRedis(ctx, redisIPs[0])
		require.NoError(t, err, step)
		require.Equal(t, "master", state.info.Role, "%s: the recorded master must never be demoted", step)
		require.EqualValues(t, datasetKeys+1, state.keys, "%s: the recorded master's dataset must not change", step)
		require.Equal(t, before[0].runID, state.runID, "%s: the recorded master must not restart", step)
	}
	converged := func() (bool, string) {
		masterState, err := labObserveRedis(ctx, redisIPs[0])
		if err != nil || masterState.info.Role != "master" || masterState.info.ConnectedSlaves != 2 {
			return false, fmt.Sprintf("recorded master role=%s connected_slaves=%d err=%v", masterState.info.Role, masterState.info.ConnectedSlaves, err)
		}
		for _, ip := range redisIPs[1:] {
			state, err := labObserveRedis(ctx, ip)
			if err != nil || state.info.Role != "slave" || state.info.MasterHost != redisIPs[0] || state.info.MasterLinkStatus != "up" || state.keys != datasetKeys+1 {
				return false, fmt.Sprintf("pod %s role=%s master_host=%s link=%s keys=%d err=%v", ip, state.info.Role, state.info.MasterHost, state.info.MasterLinkStatus, state.keys, err)
			}
		}
		for _, ip := range sentinelIPs {
			state, err := labObserveSentinel(ctx, ip)
			if err != nil || state.host != redisIPs[0] || state.status != "ok" || state.slaves != 2 || state.sentinels != 3 || !state.quorumOK || len(state.peers) != 2 {
				return false, fmt.Sprintf("sentinel %s monitors=%s status=%s slaves=%d sentinels=%d quorumOK=%v peers=%v err=%v", ip, state.host, state.status, state.slaves, state.sentinels, state.quorumOK, state.peers, err)
			}
			for peer, flags := range state.peers {
				if strings.Contains(flags, "s_down") || strings.Contains(flags, "disconnected") {
					return false, fmt.Sprintf("sentinel %s retains peer %s flags=%s", ip, peer, flags)
				}
			}
		}
		if recorded := lab.recordedMaster(ctx, t); recorded != lab.redisPodName[0] {
			return false, fmt.Sprintf("status.masterNode=%s", recorded)
		}
		for i, want := range []string{common.RedisRoleLabelMaster, common.RedisRoleLabelSlave, common.RedisRoleLabelSlave} {
			if got := lab.roleLabel(ctx, t, lab.redisPodName[i]); got != want {
				return false, fmt.Sprintf("pod %s role label=%q want %q", lab.redisPodName[i], got, want)
			}
		}
		return true, ""
	}

	passes := 20
	if wantRefusal {
		passes = 4
	}
	reason := ""
	done := false
	for pass := 1; pass <= passes && !done; pass++ {
		lab.reconcileOnce(ctx, t, pass, requireRecordedMasterIntact)
		done, reason = converged()
		if !done {
			t.Logf("pass %d: not converged: %s", pass, reason)
			time.Sleep(time.Second)
		}
	}

	if wantRefusal {
		require.False(t, done)
		for i, ip := range redisIPs {
			state, err := labObserveRedis(ctx, ip)
			require.NoError(t, err)
			require.Equal(t, "master", state.info.Role, "%s must not be reattached", ip)
			require.Equal(t, before[i].keys, state.keys, "%s must keep its dataset", ip)
			require.Equal(t, before[i].runID, state.runID)
		}
		for i, ip := range sentinelIPs {
			state, err := labObserveSentinel(ctx, ip)
			require.NoError(t, err)
			require.Equal(t, monitors[i], state.host, "Sentinel %s must keep its monitor", ip)
		}
		for _, pod := range lab.redisPodName {
			require.NotEqual(t, common.RedisRoleLabelMaster, lab.roleLabel(ctx, t, pod), "no pod may enter the writable Service")
		}
		return
	}
	require.True(t, done, "group did not converge on the recorded master after %d reconcile passes: %s", passes, reason)

	// 4. All three pods serve the recorded master's dataset, newest write
	// included; the former replica's own writes are gone with its old copy.
	for _, ip := range redisIPs {
		c := labRedisClient(ip, "6379")
		for i := 0; i < datasetKeys; i++ {
			value, err := c.Get(ctx, fmt.Sprintf("key:%d", i)).Result()
			require.NoError(t, err, "%s key:%d", ip, i)
			require.Equal(t, fmt.Sprintf("value:%d", i), value, "%s key:%d", ip, i)
		}
		value, err := c.Get(ctx, "latest").Result()
		require.NoError(t, err, "%s latest", ip)
		require.Equal(t, "write", value)
		exists, err := c.Exists(ctx, "stray:0").Result()
		require.NoError(t, err)
		require.Zero(t, exists, "%s must serve the recorded master's dataset only", ip)
		require.NoError(t, c.Close())
	}
	requireRecordedMasterIntact("after convergence")
}
