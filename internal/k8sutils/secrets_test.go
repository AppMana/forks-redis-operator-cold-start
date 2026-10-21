package k8sutils

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	common "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sClientFake "k8s.io/client-go/kubernetes/fake"
)

func TestPasswordNormalizationMatchesBootstrapMode(t *testing.T) {
	original := features.Enabled(features.GenerateConfigInInitContainer)
	t.Cleanup(func() {
		require.NoError(t, features.MutableFeatureGate.Set(fmt.Sprintf("GenerateConfigInInitContainer=%t", original)))
	})
	kube := k8sClientFake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "test"}, Data: map[string][]byte{"password": []byte(" spaced\tpassword \r\n")}})
	for _, enabled := range []bool{true, false} {
		require.NoError(t, features.MutableFeatureGate.Set(fmt.Sprintf("GenerateConfigInInitContainer=%t", enabled)))
		value, err := getRedisPassword(context.Background(), kube, "test", "credentials", "password")
		require.NoError(t, err)
		if enabled {
			require.Equal(t, " spaced\tpassword ", value)
		} else {
			require.Equal(t, "spaced\tpassword", value)
		}
	}
}

func Test_getRedisPassword(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *k8sClientFake.Clientset
		namespace   string
		secretName  string
		secretKey   string
		expected    string
		expectedErr bool
	}{
		{
			name: "successful retrieval",
			setup: func() *k8sClientFake.Clientset {
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"password": []byte("secret-password"),
					},
				}
				client := k8sClientFake.NewSimpleClientset(secret.DeepCopyObject())
				return client
			},
			namespace:   "default",
			secretName:  "redis-secret",
			secretKey:   "password",
			expected:    "secret-password",
			expectedErr: false,
		},
		{
			name: "secret not found",
			setup: func() *k8sClientFake.Clientset {
				client := k8sClientFake.NewSimpleClientset()
				return client
			},
			namespace:   "default",
			secretName:  "non-existent",
			secretKey:   "password",
			expected:    "",
			expectedErr: true,
		},
		{
			name: "secret exists but key is missing",
			setup: func() *k8sClientFake.Clientset {
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"anotherKey": []byte("some-value"),
					},
				}
				client := k8sClientFake.NewSimpleClientset(secret.DeepCopyObject())
				return client
			},
			namespace:   "default",
			secretName:  "redis-secret",
			secretKey:   "missingKey",
			expected:    "",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setup()

			got, err := getRedisPassword(context.TODO(), client, tt.namespace, tt.secretName, tt.secretKey)

			if tt.expectedErr {
				require.Error(t, err, "Expected an error but didn't get one")
			} else {
				require.NoError(t, err, "Expected no error but got one")
				assert.Equal(t, tt.expected, got, "Expected and actual values do not match")
			}
		})
	}
}

func Test_getRedisTLSConfig(t *testing.T) {
	tests := []struct {
		name         string
		setup        func() *k8sClientFake.Clientset
		redisCluster *rcvb2.RedisCluster
		redisInfo    RedisDetails
		expectTLS    bool
	}{
		{
			name: "TLS enabled and successful configuration",
			setup: func() *k8sClientFake.Clientset {
				tlsSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-tls-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"ca.crt":  helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "ca.crt")),
						"tls.crt": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.crt")),
						"tls.key": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.key")),
					},
				}
				client := k8sClientFake.NewSimpleClientset(tlsSecret)
				return client
			},
			redisCluster: &rcvb2.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-cluster",
					Namespace: "default",
				},
				Spec: rcvb2.RedisClusterSpec{
					TLS: &common.TLSConfig{
						CaCertFile:  "ca.crt",
						CertKeyFile: "tls.crt",
						KeyFile:     "tls.key",
						Secret: corev1.SecretVolumeSource{
							SecretName: "redis-tls-secret",
						},
					},
				},
			},
			redisInfo: RedisDetails{
				PodName:   "redis-pod",
				Namespace: "default",
			},
			expectTLS: true,
		},
		{
			name: "TLS enabled with no CA key and implicit CA config",
			setup: func() *k8sClientFake.Clientset {
				tlsSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-tls-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"tls.crt": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.crt")),
						"tls.key": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.key")),
					},
				}
				client := k8sClientFake.NewSimpleClientset(tlsSecret)
				return client
			},
			redisCluster: &rcvb2.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-cluster",
					Namespace: "default",
				},
				Spec: rcvb2.RedisClusterSpec{
					TLS: &common.TLSConfig{
						CertKeyFile: "tls.crt",
						KeyFile:     "tls.key",
						Secret: corev1.SecretVolumeSource{
							SecretName: "redis-tls-secret",
						},
					},
				},
			},
			redisInfo: RedisDetails{
				PodName:   "redis-pod",
				Namespace: "default",
			},
			expectTLS: true,
		},
		{
			name: "TLS enabled but secret not found",
			setup: func() *k8sClientFake.Clientset {
				client := k8sClientFake.NewSimpleClientset()
				return client
			},
			redisCluster: &rcvb2.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-cluster",
					Namespace: "default",
				},
				Spec: rcvb2.RedisClusterSpec{
					TLS: &common.TLSConfig{
						CaCertFile:  "ca.crt",
						CertKeyFile: "tls.crt",
						KeyFile:     "tls.key",
						Secret: corev1.SecretVolumeSource{
							SecretName: "redis-tls-secret",
						},
					},
				},
			},
			redisInfo: RedisDetails{
				PodName:   "redis-pod",
				Namespace: "default",
			},
			expectTLS: false,
		},
		{
			name: "TLS enabled with explicit CA configured but missing from secret",
			setup: func() *k8sClientFake.Clientset {
				tlsSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-tls-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						// ca.crt is intentionally absent, but CaCertFile is explicitly set
						"tls.crt": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.crt")),
						"tls.key": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.key")),
					},
				}
				client := k8sClientFake.NewSimpleClientset(tlsSecret)
				return client
			},
			redisCluster: &rcvb2.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-cluster",
					Namespace: "default",
				},
				Spec: rcvb2.RedisClusterSpec{
					TLS: &common.TLSConfig{
						CaCertFile:  "ca.crt", // explicitly set but missing from secret → misconfiguration
						CertKeyFile: "tls.crt",
						KeyFile:     "tls.key",
						Secret: corev1.SecretVolumeSource{
							SecretName: "redis-tls-secret",
						},
					},
				},
			},
			redisInfo: RedisDetails{
				PodName:   "redis-pod",
				Namespace: "default",
			},
			expectTLS: false, // should fail fast as misconfiguration
		},
		{
			name: "TLS enabled with no CA in secret and no explicit CA config uses system trust store",
			setup: func() *k8sClientFake.Clientset {
				tlsSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-tls-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"tls.crt": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.crt")),
						"tls.key": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "tls.key")),
					},
				}
				client := k8sClientFake.NewSimpleClientset(tlsSecret)
				return client
			},
			redisCluster: &rcvb2.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-cluster",
					Namespace: "default",
				},
				Spec: rcvb2.RedisClusterSpec{
					TLS: &common.TLSConfig{
						// CaCertFile intentionally not set → should use system trust store
						CertKeyFile: "tls.crt",
						KeyFile:     "tls.key",
						Secret: corev1.SecretVolumeSource{
							SecretName: "redis-tls-secret",
						},
					},
				},
			},
			redisInfo: RedisDetails{
				PodName:   "redis-pod",
				Namespace: "default",
			},
			expectTLS: true, // no explicit CA → system trust store fallback
		},
		{
			name: "TLS enabled but incomplete secret",
			setup: func() *k8sClientFake.Clientset {
				tlsSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "redis-tls-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"ca.crt": helperReadFile(filepath.Join("..", "..", "tests", "testdata", "secrets", "ca.crt")),
						// Missing tls.crt and tls.key
					},
				}
				client := k8sClientFake.NewSimpleClientset(tlsSecret)
				return client
			},
			redisCluster: &rcvb2.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "redis-cluster",
					Namespace: "default",
				},
				Spec: rcvb2.RedisClusterSpec{
					TLS: &common.TLSConfig{
						CaCertFile:  "ca.crt",
						CertKeyFile: "tls.crt",
						KeyFile:     "tls.key",
						Secret: corev1.SecretVolumeSource{
							SecretName: "redis-tls-secret",
						},
					},
				},
			},
			redisInfo: RedisDetails{
				PodName:   "redis-pod",
				Namespace: "default",
			},
			expectTLS: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setup()

			tlsConfig := getRedisTLSConfig(context.TODO(), client, tt.redisCluster.Namespace, tt.redisCluster.Spec.TLS)

			if tt.expectTLS {
				require.NotNil(t, tlsConfig, "Expected TLS configuration but got nil")
				require.NotEmpty(t, tlsConfig.Certificates, "TLS Certificates should not be empty")
				// Whether the CA comes from the secret or from the system trust
				// store fallback, the operator dials pods by IP/pod DNS that do
				// not match the server certificate SNI. The config must therefore
				// skip Go's default hostname verification and verify the chain
				// itself, otherwise the connection would fail. This is the
				// connection semantics the fallback path must preserve.
				require.NotNil(t, tlsConfig.RootCAs, "Root CAs should not be nil")
				require.True(t, tlsConfig.InsecureSkipVerify, "InsecureSkipVerify should be true so the default hostname check is skipped")
				require.NotNil(t, tlsConfig.VerifyPeerCertificate, "VerifyPeerCertificate must be set to verify the chain without hostname checks")
			} else if tt.redisCluster.Spec.TLS != nil && tt.redisCluster.Spec.TLS.Secret.SecretName != "" {
				require.NotNil(t, tlsConfig, "configured TLS must never fall back to plaintext")
				require.NotNil(t, tlsConfig.VerifyConnection)
				require.Error(t, tlsConfig.VerifyConnection(tls.ConnectionState{}))
			} else {
				assert.Nil(t, tlsConfig, "Expected no TLS configuration but got one")
			}
		})
	}
}

func TestEmptyTLSSecretReferenceFailsClosed(t *testing.T) {
	client := k8sClientFake.NewSimpleClientset()
	require.Nil(t, getRedisTLSConfig(context.Background(), client, "default", nil))
	config := getRedisTLSConfig(context.Background(), client, "default", &common.TLSConfig{})
	require.NotNil(t, config, "explicit TLS must not silently downgrade to plaintext")
	require.NotNil(t, config.VerifyConnection)
	require.Error(t, config.VerifyConnection(tls.ConnectionState{}))
	require.Empty(t, client.Actions(), "an empty secret reference should fail before API access")
}

func helperReadFile(filename string) []byte {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return data
}
