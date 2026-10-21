package redisreplication

import (
	"context"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestUnresolvedSentinelAuthorityBlocksColdStartFallback(t *testing.T) {
	instance := newReplicationInstanceForTest()
	checkedRecovery := false
	r := &Reconciler{
		StatefulSet: &fakeStatefulSetService{}, K8sClient: fake.NewSimpleClientset(),
		RedisReplicationTopology: topologyOf([]string{"example-replication-0", "example-replication-1"}, []string{"example-replication-2"}, nil),
		SentinelElectedMaster:    func(context.Context, *rrvb2.RedisReplication) (string, bool) { return "", true },
		Checker:                  keyCountsOf(map[string]int64{"example-replication-0": 7, "example-replication-1": 7, "example-replication-2": 7}),
		ValidateRecoveryLineage: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string, map[string]int64) error {
			checkedRecovery = true
			return nil
		},
		CreateRedisReplicationLink: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error {
			return nil
		},
	}
	result, err := r.reconcileRedis(context.Background(), instance)
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	require.False(t, checkedRecovery, "unresolved Sentinel authority must not enter operator-directed recovery")
}

func TestSentinelBootstrapRequiresPositiveUnconfiguredEvidence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answers []string
		allowed bool
	}{
		{"all placeholders", []string{"0.0.0.0:6379", "0.0.0.0:6379", "0.0.0.0:6379"}, true},
		{"all explicitly missing", []string{"", "", ""}, true},
		{"no responses", nil, false},
		{"minority placeholder", []string{"0.0.0.0:6379"}, false},
		{"minority real primary", []string{"10.0.0.1:6379"}, false},
		{"conflicting views", []string{"10.0.0.1:6379", "10.0.0.2:6379", ""}, false},
		{"placeholder plus established monitor", []string{"0.0.0.0:6379", "0.0.0.0:6379", "10.0.0.1:6379"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.allowed, sentinelsUnconfigured(tc.answers, 3)) })
	}
	_, ok := majorityAddress([]string{"", "", ""}, 3)
	require.False(t, ok, "absence of a monitor is not an election")
}
