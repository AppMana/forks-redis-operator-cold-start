package k8sutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestStatefulSetReadinessRespectsOnDelete(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy appsv1.StatefulSetUpdateStrategyType
		observed int64
		ready    int32
		want     bool
	}{
		{"on-delete pending manual replacement", appsv1.OnDeleteStatefulSetStrategyType, 2, 3, true},
		{"on-delete stale observation", appsv1.OnDeleteStatefulSetStrategyType, 1, 3, false},
		{"on-delete missing ready pod", appsv1.OnDeleteStatefulSetStrategyType, 2, 2, false},
		{"rolling update still pending", appsv1.RollingUpdateStatefulSetStrategyType, 2, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test", Generation: 2},
				Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3)), UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: tc.strategy}},
				Status:     appsv1.StatefulSetStatus{ObservedGeneration: tc.observed, Replicas: 3, ReadyReplicas: tc.ready, CurrentRevision: "old", UpdateRevision: "new"},
			}
			svc := NewStatefulSetService(fake.NewSimpleClientset(sts))
			require.Equal(t, tc.want, svc.IsStatefulSetReady(context.Background(), "test", "redis"))
		})
	}
}
