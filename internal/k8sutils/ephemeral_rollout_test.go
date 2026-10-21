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
				stored.Spec.Template.Spec.Containers = []corev1.Container{{Name: stored.Name, VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}}}
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

func TestEphemeralGuardDoesNotTrustUnrelatedClaims(t *testing.T) {
	stored := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "redis", OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: "RedisReplication", UID: "owner"}}}}
	stored.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "config"}}}
	stored.Spec.Template.Spec.Containers = []corev1.Container{{Name: "redis", VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/etc/redis"}}}}
	desired := stored.DeepCopy()
	err := guardEphemeralRollout(stored, desired, []byte(`{"spec":{"template":{"metadata":{"annotations":{"changed":"yes"}}}}}`), false)
	require.ErrorContains(t, err, "OnDelete")
}

func TestPersistentRedisStateMount(t *testing.T) {
	for _, tc := range []struct {
		name, kind, role, path                  string
		readOnly, sidecar, direct, shadow, want bool
	}{
		{name: "data claim", kind: "RedisReplication", path: "/data", want: true},
		{name: "unrelated config", kind: "RedisReplication", path: "/etc/redis"},
		{name: "sentinel config", kind: "RedisSentinel", path: "/etc/redis", want: true},
		{name: "sentinel data not config", kind: "RedisSentinel", path: "/data"},
		{name: "embedded sentinel", kind: "RedisReplication", role: "sentinel", path: "/etc/redis", want: true},
		{name: "read only", kind: "RedisReplication", path: "/data", readOnly: true},
		{name: "sidecar only", kind: "RedisReplication", path: "/data", sidecar: true},
		{name: "direct PVC", kind: "RedisReplication", path: "/data", direct: true, want: true},
		{name: "shadowed with emptyDir", kind: "RedisReplication", path: "/data", shadow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "redis", OwnerReferences: []metav1.OwnerReference{{Kind: tc.kind}}}}
			sts.Spec.Template.Labels = map[string]string{"redis_setup_type": tc.role}
			sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "state"}}}
			container := "redis"
			if tc.sidecar {
				container = "sidecar"
			}
			sts.Spec.Template.Spec.Containers = []corev1.Container{{Name: container, VolumeMounts: []corev1.VolumeMount{{Name: "state", MountPath: tc.path, ReadOnly: tc.readOnly}}}}
			if tc.direct {
				sts.Spec.VolumeClaimTemplates = nil
				sts.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "state", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "retained"}}}}
			}
			if tc.shadow {
				sts.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
			}
			require.Equal(t, tc.want, hasPersistentRedisStateMount(sts))
		})
	}
}
