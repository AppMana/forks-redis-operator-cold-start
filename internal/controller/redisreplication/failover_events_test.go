package redisreplication

import (
	"context"
	"sync"
	"testing"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReplicationForPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "redis-repl-1", Labels: map[string]string{
		"app": "redis-repl", "redis_setup_type": "replication", "role": "replication",
	}}}
	require.Equal(t, []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "redis-repl"}}}, replicationForPod(context.Background(), pod))

	sentinel := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "redis-repl-sentinel-0", Labels: map[string]string{
		"app": "redis-repl-sentinel", "redis_setup_type": "sentinel", "role": "sentinel",
	}}}
	require.Empty(t, replicationForPod(context.Background(), sentinel))
	require.Empty(t, replicationForPod(context.Background(), &corev1.Pod{}))
}

func TestMajorityAddress(t *testing.T) {
	addr, ok := majorityAddress([]string{"10.0.0.12:6379", "10.0.0.12:6379"}, 3)
	require.True(t, ok)
	require.Equal(t, "10.0.0.12:6379", addr)

	// One Sentinel still holding the old primary must not decide.
	_, ok = majorityAddress([]string{"10.0.0.11:6379", "10.0.0.12:6379"}, 3)
	require.False(t, ok)
	// Majority of the configured size, not of the Sentinels that answered.
	_, ok = majorityAddress([]string{"10.0.0.12:6379"}, 3)
	require.False(t, ok)
	_, ok = majorityAddress(nil, 0)
	require.False(t, ok)
}

func TestPodForAddress(t *testing.T) {
	now := metav1.Now()
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "redis-repl-1", DeletionTimestamp: &now}, Status: corev1.PodStatus{PodIP: "10.0.0.11"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "redis-repl-2"}, Status: corev1.PodStatus{PodIP: "10.0.0.12"}},
	}
	require.Equal(t, "redis-repl-2", podForAddress(pods, "10.0.0.12:6379"))
	require.Equal(t, "redis-repl-2", podForAddress(pods, "redis-repl-2.redis-repl-headless.team-a.svc.cluster.local:6379"))
	// The terminating old primary, or an address no pod holds, maps to nothing.
	require.Equal(t, "", podForAddress(pods, "10.0.0.11:6379"))
	require.Equal(t, "", podForAddress(pods, "10.0.0.99:6379"))
}

type subscriptions struct {
	mu     sync.Mutex
	active map[string]context.Context
	notify map[string]func()
}

func (s *subscriptions) subscribe(ctx context.Context, info *redis.ConnectionInfo, notify func()) {
	s.mu.Lock()
	s.active[info.GetAddress()] = ctx
	s.notify[info.GetAddress()] = notify
	s.mu.Unlock()
	<-ctx.Done()
}

func (s *subscriptions) get(addr string) (context.Context, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active[addr], s.notify[addr]
}

func sentinelPod(name, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name, Labels: map[string]string{
			"app": "redis-repl-sentinel", "redis_setup_type": "sentinel", "role": "sentinel",
		}},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// Sentinel announces +switch-master the moment it promotes a replica; that
// announcement, not a resync period, must wake the replication reconciler.
func TestSentinelEventsEnqueueReplication(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	require.NoError(t, rsvb2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	replication := &rrvb2.RedisReplication{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "redis-repl"}, Spec: rrvb2.RedisReplicationSpec{Size: ptr.To(int32(3))}}
	monitor := &rsvb2.RedisSentinel{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "redis-repl"},
		Spec: rsvb2.RedisSentinelSpec{Size: ptr.To(int32(3)), RedisSentinelConfig: &rsvb2.RedisSentinelConfig{RedisSentinelConfig: commonapi.RedisSentinelConfig{
			RedisReplicationName: "redis-repl", MasterGroupName: "redis-repl",
		}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		replication, monitor, sentinelPod("redis-repl-sentinel-0", "10.0.1.1"), sentinelPod("redis-repl-sentinel-1", "10.0.1.2"), sentinelPod("redis-repl-sentinel-2", ""),
	).Build()

	subs := &subscriptions{active: map[string]context.Context{}, notify: map[string]func(){}}
	events := NewSentinelEvents(c, nil)
	events.subscribe = subs.subscribe
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, events.sync(ctx))
	require.Eventually(t, func() bool {
		a, _ := subs.get("10.0.1.1:26379")
		b, _ := subs.get("10.0.1.2:26379")
		return a != nil && b != nil
	}, time.Second, time.Millisecond)

	_, notify := subs.get("10.0.1.2:26379")
	notify()
	select {
	case ev := <-events.Events():
		require.Equal(t, client.ObjectKeyFromObject(replication), client.ObjectKeyFromObject(ev.Object))
	case <-time.After(time.Second):
		t.Fatal("+switch-master did not enqueue the replication")
	}

	// A Sentinel pod that goes away loses its subscription.
	require.NoError(t, c.Delete(ctx, sentinelPod("redis-repl-sentinel-0", "10.0.1.1")))
	require.NoError(t, events.sync(ctx))
	gone, _ := subs.get("10.0.1.1:26379")
	require.Eventually(t, func() bool { return gone.Err() != nil }, time.Second, time.Millisecond)
	kept, _ := subs.get("10.0.1.2:26379")
	require.NoError(t, kept.Err())
}
