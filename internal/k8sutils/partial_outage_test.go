package k8sutils

import (
	"context"
	"errors"
	"testing"

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

// The production failure: Sentinel already promoted pod0 while pod2 was Pending.
// Discovery must publish pod0; topology mutation must still reject a partial view.
func TestPartialOutageRoleObservation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		pending, strict bool
	}{
		{name: "pending replica", pending: true},
		{name: "unreachable replica"},
		{name: "topology still requires complete observation", strict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cr := &rrvb2.RedisReplication{ObjectMeta: metav1.ObjectMeta{Name: "redis-test", Namespace: "test"}, Spec: rrvb2.RedisReplicationSpec{Size: ptr.To(int32(3))}}
			objects := []runtime.Object{&appsv1.StatefulSet{ObjectMeta: cr.ObjectMeta}}
			clients := map[string]*redis.Client{}
			var mocks []redismock.ClientMock
			for i, name := range []string{"redis-test-0", "redis-test-1", "redis-test-2"} {
				ip := "10.0.0.1"
				if i == 2 && tc.pending {
					ip = ""
				}
				objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"}, Status: corev1.PodStatus{PodIP: ip}})
				if i == 2 && tc.pending {
					continue
				}
				c, m := redismock.NewClientMock()
				clients[name] = c
				mocks = append(mocks, m)
				if i == 2 {
					m.ExpectInfo("Replication").SetErr(errors.New("replica unreachable"))
				} else {
					role := "master"
					if i == 1 {
						role = "slave"
					}
					m.ExpectInfo("Replication").SetVal("role:" + role + "\r\n")
				}
			}
			nodes, err := getRedisNodesByRole(ctx, fake.NewSimpleClientset(objects...), cr, "master", !tc.strict, func(name string) *redis.Client {
				c, ok := clients[name]
				require.True(t, ok, "must not dial a Pending pod")
				return c
			})
			if tc.strict {
				require.Error(t, err)
				require.Nil(t, nodes)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"redis-test-0"}, nodes)
			}
			for _, m := range mocks {
				require.NoError(t, m.ExpectationsWereMet())
			}
		})
	}
}
