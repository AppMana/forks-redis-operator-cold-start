package k8sutils

import (
	"fmt"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestACLSecretProjectionUsesDirectoryWithGeneratedConfig(t *testing.T) {
	original := features.Enabled(features.GenerateConfigInInitContainer)
	t.Cleanup(func() {
		require.NoError(t, features.MutableFeatureGate.Set(fmt.Sprintf("GenerateConfigInInitContainer=%t", original)))
	})
	acl := &commonapi.ACLConfig{Secret: &corev1.SecretVolumeSource{SecretName: "acl"}}
	require.NoError(t, features.MutableFeatureGate.Set("GenerateConfigInInitContainer=true"))
	mount := generateACLVolumeMount(acl, true)
	require.Empty(t, mount.SubPath, "subPath pins the old Secret inode and prevents projected updates")
	require.Equal(t, "/etc/redis/acl", mount.MountPath)
	containers := generateInitContainerDef("", "redis", initContainerParameters{}, nil, nil, containerParameters{ACLConfig: acl}, ptr.To("v7"))
	require.Contains(t, containers[0].Env, corev1.EnvVar{Name: "ACL_FILE_PATH", Value: "/etc/redis/acl/user.acl"})
	require.Contains(t, containers[0].VolumeMounts, *mount)
	// Legacy entrypoints may hard-code the original filename.
	require.NoError(t, features.MutableFeatureGate.Set("GenerateConfigInInitContainer=false"))
	mount = generateACLVolumeMount(acl, true)
	require.Equal(t, "/etc/redis/user.acl", mount.MountPath)
	require.Equal(t, "user.acl", mount.SubPath)
}
