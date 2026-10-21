package k8sutils

import (
	"context"
	"fmt"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestSentinelConfigPersistenceUsesClaimForBothContainers(t *testing.T) {
	enabled := features.Enabled(features.GenerateConfigInInitContainer)
	t.Cleanup(func() {
		require.NoError(t, features.MutableFeatureGate.Set(fmt.Sprintf("GenerateConfigInInitContainer=%t", enabled)))
	})
	require.NoError(t, features.MutableFeatureGate.Set("GenerateConfigInInitContainer=true"))
	cr := &rsvb2.RedisSentinel{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "test"},
		Spec: rsvb2.RedisSentinelSpec{
			Size:                      ptr.To(int32(3)),
			KubernetesConfig:          commonapi.KubernetesConfig{Image: "redis:7"},
			ConfigVolumeClaimTemplate: &corev1.PersistentVolumeClaim{Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}}},
		},
	}
	params := generateRedisSentinelParams(context.Background(), cr, 3, nil, nil)
	container, err := generateRedisSentinelContainerParams(context.Background(), nil, cr, nil, nil, nil)
	require.NoError(t, err)
	sts := generateStatefulSetsDef(metav1.ObjectMeta{Name: cr.GetStatefulSetName()}, params, metav1.OwnerReference{}, generateRedisSentinelInitContainerParams(cr), container, nil)
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
	require.Equal(t, common.VolumeNameConfig, sts.Spec.VolumeClaimTemplates[0].Name)
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		require.NotEqual(t, common.VolumeNameConfig, volume.Name)
	}
	require.Len(t, sts.Spec.Template.Spec.InitContainers, 1)
	for _, c := range []corev1.Container{sts.Spec.Template.Spec.Containers[0], sts.Spec.Template.Spec.InitContainers[0]} {
		require.Contains(t, c.VolumeMounts, generateConfigVolumeMount(common.VolumeNameConfig))
		require.Contains(t, c.Env, corev1.EnvVar{Name: "SENTINEL_PERSISTENCE_ENABLED", Value: "true"})
	}
	require.NoError(t, features.MutableFeatureGate.Set("GenerateConfigInInitContainer=false"))
	require.ErrorContains(t, CreateRedisSentinel(context.Background(), nil, cr, nil, nil), "requires GenerateConfigInInitContainer")
}
