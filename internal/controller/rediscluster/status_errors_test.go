package rediscluster

import (
	"context"
	"errors"
	"testing"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestClusterStatusWriteErrorsPropagate(t *testing.T) {
	resource := schema.GroupResource{Group: "redis.redis.opstreelabs.in", Resource: "redisclusters"}
	failure := errors.New("status service unavailable")
	conflict := apierrors.NewConflict(resource, "test", errors.New("stale version"))
	for _, tt := range []struct {
		name                   string
		first, second, wantErr error
		wantRequeue            bool
		wantCalls              int
	}{
		{name: "transport failure", first: failure, wantErr: failure, wantCalls: 1},
		{name: "cancelled", first: context.Canceled, wantErr: context.Canceled, wantCalls: 1},
		{name: "timeout", first: context.DeadlineExceeded, wantErr: context.DeadlineExceeded, wantCalls: 1},
		{name: "conflict then success", first: conflict, wantRequeue: true, wantCalls: 2},
		{name: "conflict then failure", first: conflict, second: failure, wantErr: failure, wantRequeue: true, wantCalls: 2},
		{name: "success", wantCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			require.NoError(t, rcvb2.AddToScheme(s))
			initial := &rcvb2.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}, Spec: rcvb2.RedisClusterSpec{ClusterSize: ptr.To(int32(3))}, Status: rcvb2.RedisClusterStatus{State: rcvb2.RedisClusterBootstrap}}
			calls := 0
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(initial).WithStatusSubresource(initial).WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					require.Equal(t, "status", sub)
					calls++
					if calls == 1 && tt.first != nil {
						return tt.first
					}
					if calls == 2 && tt.second != nil {
						return tt.second
					}
					return c.SubResource(sub).Update(ctx, obj, opts...)
				},
			}).Build()
			r := &Reconciler{Client: c}
			current := &rcvb2.RedisCluster{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(initial), current))
			requeue, err := r.updateStatus(context.Background(), current, rcvb2.RedisClusterStatus{State: rcvb2.RedisClusterReady})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantRequeue, requeue)
			require.Equal(t, tt.wantCalls, calls)
			stored := &rcvb2.RedisCluster{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(initial), stored))
			require.Equal(t, initial.Spec, stored.Spec, "status writes must preserve user spec")
			if tt.wantErr != nil {
				require.Equal(t, rcvb2.RedisClusterBootstrap, stored.Status.State)
			} else {
				require.Equal(t, rcvb2.RedisClusterReady, stored.Status.State)
			}
		})
	}
}
