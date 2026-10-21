package k8sutils

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func partialOutageReplication(pendingLast bool) (*rrvb2.RedisReplication, *fake.Clientset) {
	cr := &rrvb2.RedisReplication{ObjectMeta: metav1.ObjectMeta{Name: "redis-test", Namespace: "test"}, Spec: rrvb2.RedisReplicationSpec{Size: ptr.To(int32(3))}}
	objects := []runtime.Object{&appsv1.StatefulSet{ObjectMeta: cr.ObjectMeta}}
	for i, name := range []string{"redis-test-0", "redis-test-1", "redis-test-2"} {
		ip := "10.0.0.1"
		if i == 2 && pendingLast {
			ip = ""
		}
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"}, Status: corev1.PodStatus{PodIP: ip}})
	}
	return cr, fake.NewSimpleClientset(objects...)
}

// Sentinel already promoted pod0 while pod2 was Pending or unreachable.
// Observation must report pod0 as master; topology mutation must still reject
// a partial view.
func TestPartialOutageRoleObservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending bool
		reply   string
	}{
		{name: "pending replica", pending: true},
		{name: "unreachable replica"},
		{name: "missing role", reply: "connected_slaves:0\r\n"},
		{name: "unknown role", reply: "role:unknown\r\n"},
		{name: "contradictory role", reply: "role:master\r\nrole:slave\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cr, k8s := partialOutageReplication(tc.pending)
			clients := map[string]*redis.Client{}
			var mocks []redismock.ClientMock
			for i, name := range []string{"redis-test-0", "redis-test-1", "redis-test-2"} {
				if i == 2 && tc.pending {
					continue
				}
				c, m := redismock.NewClientMock()
				clients[name] = c
				mocks = append(mocks, m)
				switch i {
				case 0:
					m.ExpectInfo("Replication").SetVal("role:master\r\n")
				case 1:
					m.ExpectInfo("Replication").SetVal("role:slave\r\n")
				default:
					if tc.reply != "" {
						m.ExpectInfo("Replication").SetVal(tc.reply)
					} else {
						m.ExpectInfo("Replication").SetErr(errors.New("replica unreachable"))
					}
				}
			}
			roles, err := observeRedisReplicationRoles(context.Background(), k8s, cr, func(name string) *redis.Client {
				c, ok := clients[name]
				require.True(t, ok, "must not dial a Pending pod")
				return c
			})
			require.NoError(t, err)
			require.Equal(t, map[string]string{"redis-test-0": "master", "redis-test-1": "slave"}, roles)
			for _, m := range mocks {
				require.NoError(t, m.ExpectationsWereMet())
			}
		})
	}

	t.Run("topology still requires complete observation", func(t *testing.T) {
		cr, k8s := partialOutageReplication(false)
		topology, err := getRedisReplicationTopology(context.Background(), k8s, cr, func(context.Context, *corev1.Pod) (string, error) {
			return "", errors.New("replica unreachable")
		})
		require.NoError(t, err)
		require.False(t, topology.Complete())
		require.Empty(t, topology.Masters)
	})
}

func TestContradictoryRoleCannotCompleteTopology(t *testing.T) {
	cr, kube := partialOutageReplication(false)
	pods, err := kube.CoreV1().Pods(cr.Namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	for _, pod := range pods.Items {
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		_, err := kube.CoreV1().Pods(cr.Namespace).UpdateStatus(context.Background(), &pod, metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	topology, err := getRedisReplicationTopology(context.Background(), kube, cr, func(ctx context.Context, pod *corev1.Pod) (string, error) {
		c, m := redismock.NewClientMock()
		defer c.Close()
		t.Cleanup(func() { require.NoError(t, m.ExpectationsWereMet()) })
		response := map[string]string{
			"redis-test-0": "role:master\r\n",
			"redis-test-1": "role:slave\r\n",
			"redis-test-2": "role:master\r\nrole:slave\r\n",
		}[pod.Name]
		m.ExpectInfo("Replication").SetVal(response)
		return checkRedisServerRole(ctx, c, pod.Name)
	})
	require.NoError(t, err)
	require.False(t, topology.Complete(), "ambiguous replica must block topology mutation")
	require.Equal(t, []string{"redis-test-0"}, topology.Masters)
	require.Equal(t, []string{"redis-test-1"}, topology.Slaves)
	require.Equal(t, []string{"redis-test-2"}, topology.Unobserved)
}

// A hung or partitioned old primary accepts TCP but never answers. Its probe
// must neither delay the other pods' probes nor exceed RoleProbeTimeout, since
// the master Service cannot move until observation returns.
func TestHungPodRoleObservationIsBounded(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	cr, k8s := partialOutageReplication(false)
	healthy, m := redismock.NewClientMock()
	m.ExpectInfo("Replication").SetVal("role:master\r\n")
	start := time.Now()
	roles, err := observeRedisReplicationRoles(context.Background(), k8s, cr, func(name string) *redis.Client {
		if name == "redis-test-2" {
			return healthy
		}
		return redis.NewClient(&redis.Options{Addr: listener.Addr().String(), MaxRetries: -1, ContextTimeoutEnabled: true})
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"redis-test-2": "master"}, roles)
	require.Less(t, elapsed, RoleProbeTimeout+time.Second, "hung pods must be probed concurrently within RoleProbeTimeout")

	// The production client must honour that deadline too.
	client := configureRedisReplicationClient(context.Background(), k8s, cr, "redis-test-0")
	defer client.Close()
	require.True(t, client.Options().ContextTimeoutEnabled)
}
