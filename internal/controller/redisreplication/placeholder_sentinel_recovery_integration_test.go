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

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
		ns     = labNamespace
		rrName = labReplication

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
		sentinelCount = 3
	)

	startRedis := func(ip, replicaOf string) *exec.Cmd { return labStartRedis(t, ip, replicaOf, "") }
	startSentinel := func(ip, monitor string) *exec.Cmd { return labStartSentinel(t, ip, monitor) }
	redisClient := labRedisClient
	type redisState = labRedisState
	observeRedis := func(ip string) (redisState, error) { return labObserveRedis(ctx, ip) }
	observeSentinel := func(ip string) (labSentinelState, error) { return labObserveSentinel(ctx, ip) }

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

	// 3. The Kubernetes view of that state. The recorded master is one of the
	// replaced pods, and the survivor still carries its stale replica label.
	lab := newLabControllers(t, rrName+"-2",
		[3]string{survivorIP, replacement1, replacement2},
		[3]string{sentinel0IP, sentinel1IP, sentinel2IP},
		map[string]string{rrName + "-0": common.RedisRoleLabelSlave})
	ctrlClient, k8sClient := lab.ctrlClient, lab.k8sClient

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
		lab.reconcileOnce(ctx, t, pass, requireSurvivorIntact)

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
		current := &rrvb2.RedisReplication{}
		require.NoError(t, ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, current))
		require.Empty(t, current.Status.MasterNode, "the empty replacement must not stay recorded as master, and no ambiguous master may be recorded")
		for _, pod := range lab.redisPodName {
			require.NotEqual(t, common.RedisRoleLabelMaster, lab.roleLabel(ctx, t, pod), "no pod may enter the writable Service")
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
