package redisreplication

import (
	"context"
	"errors"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestHostnameMigrationPreflight(t *testing.T) {
	for _, scenario := range []string{"ready", "rolling", "unknown authority", "incomplete", "configuration failure", "unconfigured linked", "unconfigured ambiguous"} {
		t.Run(scenario, func(t *testing.T) {
			rr := persistentEmbeddedReplication()
			rr.Spec.Size = ptr.To(int32(3))
			rr.Spec.Sentinel.ResolveHostnames = "yes"
			rr.Spec.Sentinel.AnnounceHostnames = "yes"
			scheme := runtime.NewScheme()
			require.NoError(t, appsv1.AddToScheme(scheme))
			current := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: rr.Name, Namespace: rr.Namespace}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current).Build()
			sts := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Generation: 1}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3, CurrentRevision: "r", UpdateRevision: "r"}}
			topology := k8sutils.RedisReplicationTopology{Masters: []string{"redis-0"}, Slaves: []string{"redis-1", "redis-2"}}
			if scenario == "rolling" {
				sts.Status.UpdatedReplicas = 2
			}
			if scenario == "incomplete" {
				topology.Unobserved = []string{"redis-2"}
			}
			if scenario == "unconfigured ambiguous" {
				topology.Masters = []string{"redis-0", "redis-1"}
			}
			called := false
			r := &Reconciler{
				Client: c,
				RedisReplicationTopology: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
					return topology, nil
				},
				RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
					return "redis-0"
				},
				SentinelElectedMaster: func(context.Context, *rrvb2.RedisReplication) (string, bool) {
					if scenario == "unknown authority" {
						return "", true
					}
					if scenario == "unconfigured linked" || scenario == "unconfigured ambiguous" {
						return "", false
					}
					return "redis-0", true
				},
				ConfigureSentinel: func(_ context.Context, _ *rrvb2.RedisReplication, primary string) error {
					called = true
					require.Equal(t, "redis-0", primary)
					if scenario == "configuration failure" {
						return errors.New("configuration failed")
					}
					return nil
				},
			}
			err := r.prepareSentinelHostnameMigration(context.Background(), rr, sts)
			if scenario == "ready" || scenario == "unconfigured linked" {
				require.NoError(t, err)
				require.True(t, called)
			} else {
				require.Error(t, err)
				require.Equal(t, scenario == "configuration failure", called)
			}
		})
	}
}

func TestSentinelConfigurationFailureIsNotSwallowed(t *testing.T) {
	rr := persistentEmbeddedReplication()
	rr.Spec.Size = ptr.To(int32(3))
	rr.Spec.Sentinel.ResolveHostnames = "yes"
	failure := errors.New("configuration unavailable")
	svc := &fakeSentinelRedisService{hostnameError: failure}
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "sentinel-0"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "sentinel-1"}, Status: corev1.PodStatus{PodIP: "10.0.0.2"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "sentinel-2"}, Status: corev1.PodStatus{PodIP: "10.0.0.3"}},
	}
	r := &Reconciler{}
	err := r.configureSentinelPods(context.Background(), &fakeSentinelRedisClient{svc: svc}, rr, pods, "redis.example", "")
	require.ErrorIs(t, err, failure)
	require.Len(t, svc.hostnameCalls, 1, "do not advance after an unsuccessful peer reconciliation")
	require.Nil(t, svc.monitoredMaster)
	require.Error(t, r.configureSentinelPods(context.Background(), &fakeSentinelRedisClient{svc: svc}, rr, pods[:2], "redis.example", ""))
}

func TestHostnameMigrationResumesBootstrapWithoutAuthorizingRedisRollout(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "placeholder initialized", true: "repair failed"}[failed], func(t *testing.T) {
			rr := persistentEmbeddedReplication()
			rr.Spec.Size = ptr.To(int32(3))
			rr.Spec.Sentinel.ResolveHostnames = "yes"
			scheme := runtime.NewScheme()
			require.NoError(t, appsv1.AddToScheme(scheme))
			current := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: rr.Name, Namespace: rr.Namespace}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current).Build()
			sts := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Generation: 1}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3, CurrentRevision: "r", UpdateRevision: "r"}}
			topology := k8sutils.RedisReplicationTopology{Masters: []string{"redis-0"}, Slaves: []string{"redis-1", "redis-2"}}
			resumed := false
			failure := errors.New("placeholder observation failed")
			r := &Reconciler{
				Client: c,
				RedisReplicationTopology: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
					return topology, nil
				},
				SentinelElectedMaster: func(context.Context, *rrvb2.RedisReplication) (string, bool) { return "", true },
				ResumeSentinelBootstrap: func(_ context.Context, instance *rrvb2.RedisReplication, observed k8sutils.RedisReplicationTopology) (bool, error) {
					resumed = true
					require.Same(t, rr, instance)
					require.Equal(t, topology, observed)
					if failed {
						return false, failure
					}
					return true, nil
				},
				ConfigureSentinel: func(context.Context, *rrvb2.RedisReplication, string) error {
					t.Fatal("must reobserve majority before configuring other monitors")
					return nil
				},
			}
			err := r.prepareSentinelHostnameMigration(context.Background(), rr, sts)
			require.True(t, resumed)
			require.Error(t, err, "Redis rollout remains blocked until majority is reobserved")
			if failed {
				require.ErrorIs(t, err, failure)
			}
		})
	}
}

func TestHostnameMigrationBlocksRedisResourceWrites(t *testing.T) {
	rr := persistentEmbeddedReplication()
	rr.Spec.Size = ptr.To(int32(3))
	rr.Spec.Sentinel.ConfigVolumeClaimTemplate = nil
	rr.Spec.Sentinel.ResolveHostnames = "yes"
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rrvb2.AddToScheme(scheme))
	data := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: rr.Name, Namespace: rr.Namespace}}
	sentinel := newSentinelStatefulSet(rr, rr.SentinelHLService())
	sentinel.Generation = 1
	sentinel.Status = appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3, CurrentRevision: "r", UpdateRevision: "r"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rr, data, &sentinel).Build()
	r := &Reconciler{
		Client: c,
		RedisReplicationTopology: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
			return k8sutils.RedisReplicationTopology{Masters: []string{"redis-0"}, Slaves: []string{"redis-1", "redis-2"}}, nil
		},
		SentinelElectedMaster: func(context.Context, *rrvb2.RedisReplication) (string, bool) { return "", true },
	}
	// No K8sClient: attempting the Redis StatefulSet write before the
	// authority check would panic, rather than return this refusal.
	_, err := r.reconcileResources(context.Background(), rr)
	require.ErrorContains(t, err, "unresolved Sentinel authority")
}
