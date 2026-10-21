package redisreplication

import (
	"context"
	"errors"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestTopologyRepairHonorsExternalSentinelElection(t *testing.T) {
	instance := newReplicationInstanceForTest() // Separate RedisSentinel, not spec.sentinel.
	primary := "example-replication-1"
	old := "example-replication-0"
	for _, electedRoleMaster := range []bool{true, false} {
		t.Run(map[bool]string{true: "promoted", false: "promotion pending"}[electedRoleMaster], func(t *testing.T) {
			masters, slaves := []string{old, primary}, []string{"example-replication-2"}
			if !electedRoleMaster {
				masters, slaves = []string{old}, []string{primary, "example-replication-2"}
			}
			r := &Reconciler{
				K8sClient:                  fake.NewSimpleClientset(),
				Checker:                    keyCountsOf(map[string]int64{old: 4, primary: 4, "example-replication-2": 4}),
				RedisReplicationTopology:   topologyOf(masters, slaves, nil),
				SentinelElectedMaster:      func(context.Context, *rrvb2.RedisReplication) (string, bool) { return primary, true },
				RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string { return old },
			}
			master, identified := r.observedRedisReplicationMaster(context.Background(), instance, k8sutils.RedisReplicationTopology{Masters: masters, Slaves: slaves})
			if electedRoleMaster {
				require.Equal(t, primary, master)
				require.True(t, identified)
			} else {
				require.Empty(t, master)
				require.False(t, identified)
			}
			mutated := false
			r.CreateRedisReplicationLink = func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, _ []string, chosen string) error {
				require.Equal(t, primary, chosen)
				mutated = true
				return nil
			}
			_, err := r.reconcileRedis(context.Background(), instance)
			require.NoError(t, err)
			require.Equal(t, electedRoleMaster, mutated)
		})
	}
}

func TestMergedColdStartRecovery(t *testing.T) {
	for _, shape := range []string{"all replicas", "two masters and ghost replica"} {
		for _, safe := range []bool{true, false} {
			t.Run(shape+map[bool]string{true: " safe", false: " ambiguous"}[safe], func(t *testing.T) {
				instance := newReplicationInstanceForTest()
				instance.Status.MasterNode = "example-replication-1"
				pods := []string{"example-replication-0", "example-replication-1", "example-replication-2"}
				masters, slaves := []string(nil), pods
				if shape != "all replicas" {
					masters, slaves = pods[:2], pods[2:]
				}
				mutated := false
				mutate := func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, got []string, master string) error {
					require.ElementsMatch(t, pods, got)
					require.Equal(t, instance.Status.MasterNode, master)
					mutated = true
					return nil
				}
				r := &Reconciler{
					K8sClient:                  fake.NewSimpleClientset(),
					RedisReplicationTopology:   topologyOf(masters, slaves, nil),
					RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string { return "" },
					Checker:                    keyCountsOf(map[string]int64{pods[0]: 4, pods[1]: 4, pods[2]: 4}),
					ValidateRecoveryLineage: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string, map[string]int64) error {
						if !safe {
							return errors.New("divergent history")
						}
						return nil
					},
					PromoteRecoveryMaster:      mutate,
					CreateRedisReplicationLink: mutate,
				}
				_, err := r.reconcileRedis(context.Background(), instance)
				require.NoError(t, err)
				require.Equal(t, safe, mutated)
			})
		}
	}
}
