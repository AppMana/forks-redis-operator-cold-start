package k8sutils

import (
	"context"
	"errors"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type unavailableSentinelList struct {
	client.Client
	err error
}

func (c unavailableSentinelList) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return c.err
}

func TestSentinelListFailureDoesNotWriteRedisStatefulSet(t *testing.T) {
	for _, failure := range []error{errors.New("API temporarily unavailable"), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			kube := kfake.NewSimpleClientset()
			cr := &rrvb2.RedisReplication{
				ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test"},
				Spec:       rrvb2.RedisReplicationSpec{Size: ptr.To(int32(3))},
			}
			err := CreateReplicationRedis(context.Background(), cr, kube, unavailableSentinelList{err: failure})
			require.ErrorIs(t, err, failure)
			require.Empty(t, kube.Actions(), "failed topology discovery must not change the pod template")
		})
	}
}

func TestExternalSentinelHostnameSettingsIndependentOfListOrder(t *testing.T) {
	dns := rsvb2.RedisSentinel{Spec: rsvb2.RedisSentinelSpec{RedisSentinelConfig: &rsvb2.RedisSentinelConfig{}}}
	dns.Spec.RedisSentinelConfig.RedisReplicationName = "redis"
	dns.Spec.RedisSentinelConfig.ResolveHostnames = "yes"
	dns.Spec.RedisSentinelConfig.AnnounceHostnames = "yes"
	ip := *dns.DeepCopy()
	ip.Spec.RedisSentinelConfig.ResolveHostnames = "no"
	ip.Spec.RedisSentinelConfig.AnnounceHostnames = "no"
	for _, groups := range [][]rsvb2.RedisSentinel{{dns, ip}, {ip, dns}} {
		resolve, announce := externalSentinelHostnameSettings(groups, "redis")
		require.Equal(t, "yes", resolve)
		require.Equal(t, "no", announce, "IP-only Sentinel must be able to discover replicas")
	}
	resolve, announce := externalSentinelHostnameSettings([]rsvb2.RedisSentinel{dns, dns}, "redis")
	require.Equal(t, "yes", resolve)
	require.Equal(t, "yes", announce)
	resolve, announce = externalSentinelHostnameSettings([]rsvb2.RedisSentinel{ip, ip}, "redis")
	require.Equal(t, "no", resolve)
	require.Equal(t, "no", announce)
	resolve, announce = externalSentinelHostnameSettings([]rsvb2.RedisSentinel{dns}, "unrelated")
	require.Equal(t, "no", resolve)
	require.Equal(t, "no", announce)
	resolve, announce = externalSentinelHostnameSettings(nil, "redis")
	require.Equal(t, "no", resolve)
	require.Equal(t, "no", announce)
	resolveOnly := *dns.DeepCopy()
	resolveOnly.Spec.RedisSentinelConfig.AnnounceHostnames = "no"
	for _, groups := range [][]rsvb2.RedisSentinel{{dns, resolveOnly}, {resolveOnly, dns}} {
		resolve, announce = externalSentinelHostnameSettings(groups, "redis")
		require.Equal(t, "yes", resolve)
		require.Equal(t, "yes", announce, "every group can resolve the announced hostname")
	}
	unrelated := *ip.DeepCopy()
	unrelated.Spec.RedisSentinelConfig.RedisReplicationName = "other"
	resolve, announce = externalSentinelHostnameSettings([]rsvb2.RedisSentinel{unrelated, {}, dns}, "redis")
	require.Equal(t, "yes", resolve)
	require.Equal(t, "yes", announce, "unrelated and unconfigured groups do not change the pod template")
}
