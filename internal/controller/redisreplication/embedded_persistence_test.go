package redisreplication

import (
	"context"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEmbeddedSentinelHonorsUpdateStrategy(t *testing.T) {
	for _, strategy := range []appsv1.StatefulSetUpdateStrategy{
		{Type: appsv1.OnDeleteStatefulSetStrategyType},
		{Type: appsv1.RollingUpdateStatefulSetStrategyType, RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))}},
	} {
		t.Run(string(strategy.Type), func(t *testing.T) {
			rr := persistentEmbeddedReplication()
			rr.Spec.Sentinel.UpdateStrategy = strategy
			sts := newSentinelStatefulSet(rr, rr.SentinelHLService())
			require.Equal(t, strategy, sts.Spec.UpdateStrategy)
			if sts.Spec.UpdateStrategy.RollingUpdate != nil {
				*sts.Spec.UpdateStrategy.RollingUpdate.Partition = 0
				require.Equal(t, int32(2), *rr.Spec.Sentinel.UpdateStrategy.RollingUpdate.Partition)
			}
		})
	}
}

func persistentEmbeddedReplication() *rrvb2.RedisReplication {
	return &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test"},
		Spec: rrvb2.RedisReplicationSpec{Sentinel: &rrvb2.Sentinel{
			Size:                      3,
			ConfigVolumeClaimTemplate: &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "user-name"}},
		}},
	}
}

func TestEmbeddedSentinelPersistentTemplate(t *testing.T) {
	rr := persistentEmbeddedReplication()
	sts := newSentinelStatefulSet(rr, rr.SentinelHLService())
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
	require.Equal(t, "config", sts.Spec.VolumeClaimTemplates[0].Name)
	require.Equal(t, "user-name", rr.Spec.Sentinel.ConfigVolumeClaimTemplate.Name, "do not mutate the cached CR")
	pod := sts.Spec.Template.Spec
	require.Len(t, pod.InitContainers, 1)
	require.Equal(t, []string{"bootstrap", "--sentinel"}, pod.InitContainers[0].Args)
	require.Contains(t, pod.InitContainers[0].Env, corev1.EnvVar{Name: "SENTINEL_PERSISTENCE_ENABLED", Value: "true"})
	require.Equal(t, pod.InitContainers[0].VolumeMounts, pod.Containers[0].VolumeMounts)
	require.Equal(t, []string{"redis-sentinel"}, pod.Containers[0].Command)
	require.Equal(t, []string{"/etc/redis/sentinel.conf"}, pod.Containers[0].Args)
	require.Empty(t, pod.Volumes, "no emptyDir may shadow the durable config claim")
	rr.Spec.Sentinel.ConfigVolumeClaimTemplate = nil
	legacy := newSentinelStatefulSet(rr, rr.SentinelHLService())
	require.Empty(t, legacy.Spec.VolumeClaimTemplates)
	require.Empty(t, legacy.Spec.Template.Spec.InitContainers)
	require.Empty(t, legacy.Spec.Template.Spec.Containers[0].Command)
}

func TestEmbeddedSentinelStorageMigrationGuard(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.GenerateConfigInInitContainer, true)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	rr := persistentEmbeddedReplication()
	current := newSentinelStatefulSet(rr, rr.SentinelHLService())
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&current).Build()
	r := &Reconciler{Client: c}
	require.NoError(t, r.validateSentinelConfigStorage(context.Background(), rr))
	rr.Spec.Sentinel.ConfigVolumeClaimTemplate = nil
	require.ErrorContains(t, r.validateSentinelConfigStorage(context.Background(), rr), "cannot be changed in place")
	current.Spec.VolumeClaimTemplates = nil
	require.NoError(t, c.Update(context.Background(), &current))
	rr = persistentEmbeddedReplication()
	require.ErrorContains(t, r.validateSentinelConfigStorage(context.Background(), rr), "cannot be changed in place")
	// No Kubernetes client is supplied: reaching any Redis resource write
	// before this guard would panic rather than return the migration error.
	_, err := r.reconcileResources(context.Background(), rr)
	require.ErrorContains(t, err, "cannot be changed in place")
	require.NoError(t, c.Delete(context.Background(), &current))
	require.NoError(t, r.validateSentinelConfigStorage(context.Background(), rr), "fresh installations are allowed")
}

func TestEmbeddedSentinelPersistenceRequiresConfigGenerator(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.GenerateConfigInInitContainer, false)
	require.ErrorContains(t, (&Reconciler{}).validateSentinelConfigStorage(context.Background(), persistentEmbeddedReplication()), "GenerateConfigInInitContainer")
}
