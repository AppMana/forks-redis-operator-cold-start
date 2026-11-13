package redisreplication

import (
	"context"
	"errors"
	"testing"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/stretchr/testify/require"
)

const deadRedisIP = "10.0.8.8"

// newRestartedMastersScenario is the state left behind when every Redis pod of
// a persistent three-pod replication restarted from its own disk and the
// Sentinels lost their configuration:
//
//   - every pod is a master holding a copy of the dataset, with no attached
//     replicas and a replication ID of its own, so no two histories compare,
//   - the offsets only count what each pod did since it started, and the
//     recorded master (pod 0) restarted last, so its offset is the lowest,
//   - every Sentinel monitors the unspecified-address placeholder and knows no
//     other Sentinel.
func newRestartedMastersScenario() *placeholderScenario {
	return &placeholderScenario{
		masters: []string{podName(0), podName(1), podName(2)},
		counts:  map[string]int64{podName(0): 1053, podName(1): 7695, podName(2): 7001},
		infos: map[string]k8sutils.ReplicationNodeInfo{
			podName(0): {Role: "master", ReplID: "a1", MasterReplOffset: 12},
			podName(1): {Role: "master", ReplID: "b2", MasterReplOffset: 6160},
			podName(2): {Role: "master", ReplID: "c3", MasterReplOffset: 5900},
		},
		sentinels: [3]sentinelOpinion{
			{master: placeholderAddr, known: 1},
			{master: placeholderAddr, known: 1},
			{master: placeholderAddr, known: 1},
		},
		// Replication IDs that differ can never satisfy the lineage rules.
		lineageErr: errors.New("ambiguous recovery history"),
		recorded:   podName(0),
	}
}

// One Sentinel still monitors the address the recorded master had before it
// restarted; that address now belongs to no pod.
func oneSentinelOnDeadAddress(s *placeholderScenario) {
	s.sentinels[0] = sentinelOpinion{master: deadRedisIP, known: 1}
}

// One Sentinel monitors the recorded master, which never restarted, while the
// other two pods restarted from their disks.
func oneSentinelOnRecordedMaster(s *placeholderScenario) {
	s.sentinels[0] = sentinelOpinion{master: podIP(0), known: 1}
}

func TestReconcileRedisRestoresRecordedMasterAmongRestartedMasters(t *testing.T) {
	for name, arrange := range map[string]func(*placeholderScenario){
		"every Sentinel on the placeholder":                           func(*placeholderScenario) {},
		"one Sentinel on a dead address, two on the placeholder":      oneSentinelOnDeadAddress,
		"one Sentinel on the recorded master, two on the placeholder": oneSentinelOnRecordedMaster,
		"Sentinels monitor nothing at all": func(s *placeholderScenario) {
			for i := range s.sentinels {
				s.sentinels[i] = sentinelOpinion{unmonitored: true}
			}
		},
		"the other masters are empty but have a replication offset": func(s *placeholderScenario) {
			s.counts[podName(1)], s.counts[podName(2)] = 0, 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			scenario := newRestartedMastersScenario()
			arrange(scenario)
			h := scenario.build(t)
			_, err := h.reconciler.reconcileRedis(context.Background(), h.instance)
			require.NoError(t, err)
			require.Equal(t, podName(0), h.linkedMaster, "the recorded master must become the master of the others")
			require.ElementsMatch(t, []string{podName(0), podName(1), podName(2)}, h.linked)
			require.Empty(t, h.promoted, "a pod that already serves as master must not be re-promoted")
		})
	}
}

func TestReconcileRedisKeepsRefusingRestartedMastersWithoutARecord(t *testing.T) {
	for name, arrange := range map[string]func(*placeholderScenario){
		"no master is recorded":          func(s *placeholderScenario) { s.recorded = "" },
		"the recorded master is empty":   func(s *placeholderScenario) { s.counts[podName(0)] = 0 },
		"a key count is missing":         func(s *placeholderScenario) { delete(s.counts, podName(2)) },
		"a replication state is missing": func(s *placeholderScenario) { delete(s.infos, podName(1)) },
		"a Redis pod is unobserved": func(s *placeholderScenario) {
			s.masters, s.unobserved = []string{podName(0), podName(1)}, []string{podName(2)}
		},
		"a replica holds data on a different replication ID": func(s *placeholderScenario) {
			s.masters, s.slaves = []string{podName(0), podName(1)}, []string{podName(2)}
			s.infos[podName(2)] = k8sutils.ReplicationNodeInfo{Role: "slave", ReplID: "c3", SlaveReplOffset: 5900, MasterLinkStatus: "down"}
		},
		"a Sentinel does not answer": func(s *placeholderScenario) {
			s.sentinels[2] = sentinelOpinion{unreachable: true}
		},
		"a Sentinel does not answer beside one on a dead address": func(s *placeholderScenario) {
			oneSentinelOnDeadAddress(s)
			s.sentinels[2] = sentinelOpinion{unreachable: true}
		},
		"a Sentinel monitors another live pod": func(s *placeholderScenario) {
			s.sentinels[1] = sentinelOpinion{master: podIP(1), known: 1}
		},
		"a Sentinel monitors another live pod beside one on a dead address": func(s *placeholderScenario) {
			oneSentinelOnDeadAddress(s)
			s.sentinels[1] = sentinelOpinion{master: podIP(1), known: 1}
		},
		"Sentinels disagree on the recorded master and another live pod": func(s *placeholderScenario) {
			oneSentinelOnRecordedMaster(s)
			s.sentinels[1] = sentinelOpinion{master: podIP(1), known: 1}
		},
	} {
		t.Run(name, func(t *testing.T) {
			scenario := newRestartedMastersScenario()
			arrange(scenario)
			h := scenario.build(t)
			_, err := h.reconciler.reconcileRedis(context.Background(), h.instance)
			require.NoError(t, err)
			require.Empty(t, h.linkedMaster, "no pod may be reattached")
			require.Empty(t, h.promoted, "no pod may be promoted")
		})
	}
}

// A Sentinel majority that agrees on a live master outranks the record: the
// operator follows that election and never elects the recorded pod over it.
func TestReconcileRedisFollowsSentinelQuorumOverRecordedMaster(t *testing.T) {
	scenario := newRestartedMastersScenario()
	scenario.sentinels[1] = sentinelOpinion{master: podIP(1), known: 3}
	scenario.sentinels[2] = sentinelOpinion{master: podIP(1), known: 3}
	h := scenario.build(t)

	master, authoritative := h.reconciler.sentinelElectedMaster(context.Background(), h.instance)
	require.True(t, authoritative)
	require.Equal(t, podName(1), master)

	_, err := h.reconciler.reconcileRedis(context.Background(), h.instance)
	require.NoError(t, err)
	require.Equal(t, podName(1), h.linkedMaster, "the Sentinel-elected master must stay the master")
	require.Empty(t, h.promoted)
}

func TestReconcileStatusPublishesRecordedMasterNamedByItsSentinel(t *testing.T) {
	scenario := newRestartedMastersScenario()
	oneSentinelOnRecordedMaster(scenario)
	h := scenario.build(t)
	_, err := h.reconciler.reconcileStatus(context.Background(), h.instance)
	require.NoError(t, err)
	require.Equal(t, podName(0), h.recordedMaster(t))
	require.NotNil(t, h.healer.published)
	require.Equal(t, podName(0), *h.healer.published)
}

// Until the pods are reattached no Sentinel names a live master, so no pod may
// enter the writable Service and the record that recovery depends on is kept.
func TestReconcileStatusKeepsRecordOfRestartedMasters(t *testing.T) {
	for name, arrange := range map[string]func(*placeholderScenario){
		"every Sentinel on the placeholder": func(*placeholderScenario) {},
		"one Sentinel on a dead address":    oneSentinelOnDeadAddress,
	} {
		t.Run(name, func(t *testing.T) {
			scenario := newRestartedMastersScenario()
			arrange(scenario)
			h := scenario.build(t)
			_, err := h.reconciler.reconcileStatus(context.Background(), h.instance)
			require.NoError(t, err)
			require.Equal(t, podName(0), h.recordedMaster(t))
			require.NotNil(t, h.healer.published)
			require.Empty(t, *h.healer.published)
		})
	}
}
