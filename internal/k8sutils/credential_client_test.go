package k8sutils

import (
	"context"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redis/v1beta2"
	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestCredentialFailureClientsNeverDial(t *testing.T) {
	for _, kind := range []string{"cluster", "standalone", "replication"} {
		t.Run(kind, func(t *testing.T) {
			for _, ref := range []*commonapi.ExistingPasswordSecret{
				{},
				{Name: ptr.To("credentials")},
				{Name: ptr.To(""), Key: ptr.To("password")},
				{Name: ptr.To("credentials"), Key: ptr.To("")},
				{Name: ptr.To("absent"), Key: ptr.To("password")},
				{Name: ptr.To("credentials"), Key: ptr.To("missing")},
			} {
				kube := k8sfake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "test"}})
				config := commonapi.KubernetesConfig{ExistingPasswordSecret: ref}
				meta := metav1.ObjectMeta{Name: "upgrade", Namespace: "test"}
				var client *redis.Client
				require.NotPanics(t, func() {
					switch kind {
					case "cluster":
						cr := &rcvb2.RedisCluster{ObjectMeta: meta}
						cr.Spec.KubernetesConfig = config
						cr.Spec.Port = ptr.To(6379)
						client = configureRedisClient(context.Background(), kube, cr, "upgrade-0")
					case "standalone":
						cr := &rvb2.Redis{ObjectMeta: meta}
						cr.Spec.KubernetesConfig = config
						client = configureRedisStandaloneClient(context.Background(), kube, cr, "upgrade-0")
					case "replication":
						cr := &rrvb2.RedisReplication{ObjectMeta: meta}
						cr.Spec.KubernetesConfig = config
						client = configureRedisReplicationClientForAddress(context.Background(), kube, cr, RedisDetails{PodName: "upgrade-0", Namespace: "test"}, "127.0.0.1")
					}
				})
				require.NotNil(t, client.Options().Dialer)
				conn, err := client.Options().Dialer(context.Background(), "tcp", "invalid-unused-address")
				require.Nil(t, conn)
				require.ErrorContains(t, err, "credential")
				require.ErrorContains(t, client.Ping(context.Background()).Err(), "credential")
				require.NoError(t, client.Close())
				for _, action := range kube.Actions() {
					require.Equal(t, "secrets", action.GetResource().Resource, "must stop before pod discovery")
				}
			}
		})
	}
}

func TestClusterAuthArgsRejectIncompleteReference(t *testing.T) {
	kube := k8sfake.NewSimpleClientset()
	cr := &rcvb2.RedisCluster{}
	cr.Spec.KubernetesConfig.ExistingPasswordSecret = &commonapi.ExistingPasswordSecret{}
	var err error
	require.NotPanics(t, func() { _, err = getRedisClusterAuthArgs(context.Background(), kube, cr, "upgrade-0") })
	require.Error(t, err)
	require.Empty(t, kube.Actions())
}

func TestPasswordFromReferencePreservesUnauthenticatedAndValidModes(t *testing.T) {
	kube := k8sfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "test"},
		Data:       map[string][]byte{"password": []byte("synthetic-password\n")},
	})
	value, err := passwordFromReference(context.Background(), kube, "test", nil)
	require.NoError(t, err)
	require.Empty(t, value)
	require.Empty(t, kube.Actions())
	value, err = passwordFromReference(context.Background(), kube, "test", &commonapi.ExistingPasswordSecret{Name: ptr.To("credentials"), Key: ptr.To("password")})
	require.NoError(t, err)
	require.Equal(t, "synthetic-password", value)
}
