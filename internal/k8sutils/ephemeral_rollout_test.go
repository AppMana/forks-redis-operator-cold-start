package k8sutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestEphemeralTemplateUpdateRequiresHold(t *testing.T) {
	for _, kind := range []string{"RedisReplication", "RedisSentinel"} {
		for _, held := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/rolling", true: "/held"}[held], func(t *testing.T) {
				stored := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test", OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: kind, Name: "redis", UID: "owner"}}},
					Spec:       appsv1.StatefulSetSpec{UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis", Image: "redis:old"}}}}},
				}
				desired := stored.DeepCopy()
				desired.Spec.Template.Spec.Containers[0].Image = "redis:new"
				if held {
					desired.Spec.UpdateStrategy.Type = appsv1.OnDeleteStatefulSetStrategyType
				}
				client := fake.NewSimpleClientset(stored)
				err := patchStatefulSet(context.Background(), stored, desired, "test", false, nil, client)
				if held {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "OnDelete")
					for _, action := range client.Actions() {
						require.NotContains(t, []string{"update", "patch", "delete"}, action.GetVerb())
					}
				}
			})
		}
	}
}

func TestEphemeralRolloutGuardBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                                                     string
		persistent, held, recreate, pending, template, wantError bool
	}{
		{name: "metadata only"},
		{name: "persistent unchanged policy", persistent: true, template: true},
		{name: "held template", held: true, template: true},
		{name: "rolling template", template: true, wantError: true},
		{name: "held recreation still unsafe", held: true, recreate: true, wantError: true},
		{name: "releasing pending revision", pending: true, wantError: true},
		{name: "keeping pending revision held", pending: true, held: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stored := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: "RedisReplication", UID: "owner"}}}}
			if tc.persistent {
				stored.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}}
			}
			if tc.pending {
				stored.Status.CurrentRevision, stored.Status.UpdateRevision = "old", "new"
			}
			desired := stored.DeepCopy()
			if tc.held {
				desired.Spec.UpdateStrategy.Type = appsv1.OnDeleteStatefulSetStrategyType
			}
			changes := []byte(`{"metadata":{"annotations":{"test":"value"}}}`)
			if tc.template {
				changes = []byte(`{"spec":{"template":{"spec":{"containers":[]}}}}`)
			}
			err := guardEphemeralRollout(stored, desired, changes, tc.recreate)
			if tc.wantError {
				require.ErrorContains(t, err, "OnDelete")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
