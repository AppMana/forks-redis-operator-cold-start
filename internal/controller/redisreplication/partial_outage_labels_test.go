package redisreplication

import (
	"context"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// recordingRoleHealer records which pod the reconciler put into the master
// Service. published is nil when no label publication happened at all.
type recordingRoleHealer struct {
	redishealer.Healer
	published *string
	roles     map[string]string
}

func (h *recordingRoleHealer) UpdateRedisRoleLabel(_ context.Context, _ string, _ map[string]string, _ *commonapi.ExistingPasswordSecret, _ *commonapi.TLSConfig, observedMaster ...string) error {
	master := ""
	if len(observedMaster) > 0 {
		master = observedMaster[0]
	}
	h.published = &master
	return nil
}

func (h *recordingRoleHealer) PublishReplicationRoleLabels(_ context.Context, _ string, _ map[string]string, roles map[string]string, master string) error {
	h.published = &master
	h.roles = roles
	return nil
}

func TestReconcileStatusPublishesElectedMaster(t *testing.T) {
	for _, tc := range []struct {
		name         string
		masters      []string
		slaves       []string
		realMaster   string
		sentinel     string
		sentinelOK   bool
		statusMaster string
		published    string
		recorded     string
	}{
		{
			// Sentinel has promoted redis-repl-2 and redis-repl-1 is back as an
			// empty second master. The master Service must not wait for a
			// later reconcile to reslave redis-repl-1.
			name:         "sentinel-elected master while an empty replacement reports master",
			masters:      []string{"redis-repl-1", "redis-repl-2"},
			slaves:       []string{"redis-repl-0"},
			sentinel:     "redis-repl-2",
			sentinelOK:   true,
			statusMaster: "redis-repl-1",
			published:    "redis-repl-2",
			recorded:     "redis-repl-2",
		},
		{
			name:         "sentinel-elected master while the old master pod is missing",
			masters:      []string{"redis-repl-2"},
			slaves:       []string{"redis-repl-0"},
			sentinel:     "redis-repl-2",
			sentinelOK:   true,
			statusMaster: "redis-repl-1",
			published:    "redis-repl-2",
			recorded:     "redis-repl-2",
		},
		{
			// Sentinel still names the dead primary's address; the replacement
			// pod that reuses its name is empty and must not become writable.
			name:         "sentinel has not failed over yet",
			masters:      []string{"redis-repl-1"},
			slaves:       []string{"redis-repl-0", "redis-repl-2"},
			sentinel:     "",
			sentinelOK:   true,
			statusMaster: "redis-repl-1",
			published:    "",
			recorded:     "redis-repl-1",
		},
		{
			name:         "without sentinel, a lone master of a partial view is not trusted",
			masters:      []string{"redis-repl-1"},
			slaves:       nil,
			statusMaster: "redis-repl-2",
			published:    "",
			recorded:     "redis-repl-2",
		},
		{
			name:         "without sentinel, the master with attached replicas is published",
			masters:      []string{"redis-repl-1", "redis-repl-2"},
			slaves:       []string{"redis-repl-0"},
			realMaster:   "redis-repl-2",
			statusMaster: "redis-repl-1",
			published:    "redis-repl-2",
			recorded:     "redis-repl-2",
		},
		{
			name:         "without sentinel, a complete view with one master is published",
			masters:      []string{"redis-repl-0"},
			slaves:       []string{"redis-repl-1", "redis-repl-2"},
			statusMaster: "redis-repl-0",
			published:    "redis-repl-0",
			recorded:     "redis-repl-0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, rrvb2.AddToScheme(scheme))
			instance := &rrvb2.RedisReplication{
				ObjectMeta: metav1.ObjectMeta{Name: "redis-repl", Namespace: "team-a"},
				Spec:       rrvb2.RedisReplicationSpec{Size: ptr.To(int32(3))},
				Status:     rrvb2.RedisReplicationStatus{MasterNode: tc.statusMaster},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).WithStatusSubresource(instance).Build()
			h := &recordingRoleHealer{}
			r := &Reconciler{
				Client:  c,
				Healer:  h,
				Checker: keyCountsOf(map[string]int64{"redis-repl-0": 10, "redis-repl-1": 10, "redis-repl-2": 10}),
				ObserveRoles: func(context.Context, *rrvb2.RedisReplication) (map[string]string, error) {
					roles := map[string]string{}
					for _, pod := range tc.masters {
						roles[pod] = "master"
					}
					for _, pod := range tc.slaves {
						roles[pod] = "slave"
					}
					return roles, nil
				},
				RealMaster: func(context.Context, *rrvb2.RedisReplication, []string) string { return tc.realMaster },
				SentinelElectedMaster: func(context.Context, *rrvb2.RedisReplication) (string, bool) {
					return tc.sentinel, tc.sentinelOK
				},
			}
			_, err := r.reconcileStatus(context.Background(), instance)
			require.NoError(t, err)
			require.NotNil(t, h.published, "role labels were not published")
			require.Equal(t, tc.published, *h.published)
			require.Len(t, h.roles, len(tc.masters)+len(tc.slaves), "labels must be published from the same observation")

			stored := &rrvb2.RedisReplication{}
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "redis-repl"}, stored))
			require.Equal(t, tc.recorded, stored.Status.MasterNode)
		})
	}
}
