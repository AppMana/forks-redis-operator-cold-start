package redisreplication

import (
	"context"
	"testing"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestStatusDoesNotPublishEmptySentinelPrimary(t *testing.T) {
	for _, peerKnown := range []bool{true, false} {
		t.Run(map[bool]string{true: "peer retains data", false: "peer unavailable"}[peerKnown], func(t *testing.T) {
			instance := newReplicationInstanceForTest()
			primary := instance.Name + "-0"
			instance.Status.MasterNode = primary
			scheme := runtime.NewScheme()
			require.NoError(t, rrvb2.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).WithStatusSubresource(instance).Build()
			counts := map[string]int64{primary: 0}
			if peerKnown {
				counts[instance.Name+"-1"] = 10
				counts[instance.Name+"-2"] = 10
			}
			h := &recordingRoleHealer{}
			r := &Reconciler{
				Client: c, Healer: h, Checker: keyCountsOf(counts),
				ObserveRoles: func(context.Context, *rrvb2.RedisReplication) (map[string]string, error) {
					return map[string]string{primary: "master", instance.Name + "-1": "slave", instance.Name + "-2": "slave"}, nil
				},
				// Sentinel still names the FQDN reused by an empty replacement.
				SentinelElectedMaster: func(context.Context, *rrvb2.RedisReplication) (string, bool) { return primary, true },
			}
			result, err := r.reconcileStatus(context.Background(), instance)
			require.NoError(t, err)
			require.NotNil(t, h.published)
			require.Empty(t, *h.published, "empty replacement must not enter the writable Service")
			require.Equal(t, redishealer.UnsafeMasterRole, h.roles[primary], "withdraw an existing unsafe label too")
			require.Positive(t, result.RequeueAfter)
		})
	}
}

func TestPublicationDataGuard(t *testing.T) {
	for _, tc := range []struct {
		name            string
		counts          map[string]int64
		publish, reject bool
	}{
		{"fresh empty cluster", map[string]int64{"example-replication-0": 0, "example-replication-1": 0, "example-replication-2": 0}, true, false},
		{"nonempty primary with missing peers", map[string]int64{"example-replication-0": 5}, true, false},
		{"unknown primary", map[string]int64{"example-replication-1": 5}, false, false},
		{"empty primary and missing peer", map[string]int64{"example-replication-0": 0, "example-replication-1": 0}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Reconciler{Checker: keyCountsOf(tc.counts)}
			publish, reject := r.masterDataSafeForPublication(context.Background(), newReplicationInstanceForTest(), "example-replication-0")
			require.Equal(t, tc.publish, publish)
			require.Equal(t, tc.reject, reject)
		})
	}
}

type blockedPublicationChecker struct{ redishealer.Checker }

type delayedMetadataChecker struct{ redishealer.Checker }

func (delayedMetadataChecker) GetReplicationKeyCounts(ctx context.Context, _ *rrvb2.RedisReplication, pods []string) map[string]int64 {
	select {
	case <-time.After(1100 * time.Millisecond):
		return map[string]int64{pods[0]: 1}
	case <-ctx.Done():
		return nil
	}
}

func TestPublicationAllowsMetadataLookupBeforeRedisProbe(t *testing.T) {
	r := &Reconciler{Checker: delayedMetadataChecker{}}
	publish, reject := r.masterDataSafeForPublication(context.Background(), newReplicationInstanceForTest(), "example-replication-0")
	require.True(t, publish, "Kubernetes rate limiting must not consume the entire Redis probe budget")
	require.False(t, reject)
}

func (blockedPublicationChecker) GetReplicationKeyCounts(ctx context.Context, _ *rrvb2.RedisReplication, _ []string) map[string]int64 {
	<-ctx.Done()
	return nil
}

func TestPublicationProbeHasDeadline(t *testing.T) {
	r := &Reconciler{Checker: blockedPublicationChecker{}}
	start := time.Now()
	publish, reject := r.masterDataSafeForPublication(context.Background(), newReplicationInstanceForTest(), "example-replication-0")
	require.False(t, publish)
	require.False(t, reject)
	require.Less(t, time.Since(start), 6*time.Second)
}
