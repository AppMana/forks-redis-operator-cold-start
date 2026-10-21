package redis

import (
	"context"
	"crypto/tls"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestMissingTLSSecretMustNotEnablePlaintext(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "redis-0"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}}
	config := &commonapi.TLSConfig{Secret: corev1.SecretVolumeSource{SecretName: "missing-cert"}}
	conn := createConnectionInfo(context.Background(), pod, "synthetic-only", config, k8sfake.NewSimpleClientset(), "test", "6379")
	require.NotNil(t, conn.TLSConfig, "nil TLS config makes go-redis send plaintext, including authentication")
	require.NotNil(t, conn.TLSConfig.VerifyConnection)
	require.Error(t, conn.TLSConfig.VerifyConnection(tls.ConnectionState{}))
}
