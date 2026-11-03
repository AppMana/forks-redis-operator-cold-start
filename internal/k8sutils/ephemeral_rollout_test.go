package k8sutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestEphemeralTemplateUpdateAutomaticallyPacesMultiPodRollout(t *testing.T) {
	for _, kind := range []string{"RedisCluster", "RedisReplication", "RedisSentinel"} {
		t.Run(kind, func(t *testing.T) {
			replicas := int32(3)
			stored := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "redis",
					Namespace:       "test",
					Generation:      1,
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: kind, Name: "redis", UID: "owner"}},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:       &replicas,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
					Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis", Image: "redis:old"}}}},
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 1,
					Replicas:           3,
					ReadyReplicas:      3,
					UpdatedReplicas:    3,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-1",
				},
			}
			desired := stored.DeepCopy()
			desired.Spec.Template.Spec.Containers[0].Image = "redis:new"

			// Step 1: Template changes -> operator automatically sets Partition = 2 (replicas - 1)
			client := fake.NewSimpleClientset(stored)
			require.NoError(t, patchStatefulSet(context.Background(), stored, desired.DeepCopy(), "test", false, nil, client))
			updated, err := client.AppsV1().StatefulSets("test").Get(context.Background(), "redis", metav1.GetOptions{})
			require.NoError(t, err)
			require.NotNil(t, updated.Spec.UpdateStrategy.RollingUpdate)
			require.NotNil(t, updated.Spec.UpdateStrategy.RollingUpdate.Partition)
			require.Equal(t, int32(2), *updated.Spec.UpdateStrategy.RollingUpdate.Partition)

			// Step 2: Pod-2 updated and Ready (UpdatedReplicas=1, ReadyReplicas=3) -> Partition steps down to 1
			updated.Generation = 2
			updated.Status.ObservedGeneration = 2
			updated.Status.CurrentRevision = "rev-1"
			updated.Status.UpdateRevision = "rev-2"
			updated.Status.UpdatedReplicas = 1
			updated.Status.ReadyReplicas = 3
			nextDesired := desired.DeepCopy()
			nextDesired.Spec.UpdateStrategy.RollingUpdate = nil
			require.NoError(t, patchStatefulSet(context.Background(), updated, nextDesired, "test", false, nil, client))
			updated2, err := client.AppsV1().StatefulSets("test").Get(context.Background(), "redis", metav1.GetOptions{})
			require.NoError(t, err)
			require.NotNil(t, updated2.Spec.UpdateStrategy.RollingUpdate)
			require.NotNil(t, updated2.Spec.UpdateStrategy.RollingUpdate.Partition)
			require.Equal(t, int32(1), *updated2.Spec.UpdateStrategy.RollingUpdate.Partition)

			// Step 3: Pod-1 updated and Ready (UpdatedReplicas=2, ReadyReplicas=3) -> Partition clears (nil / 0)
			updated2.Generation = 3
			updated2.Status.ObservedGeneration = 3
			updated2.Status.UpdatedReplicas = 2
			updated2.Status.ReadyReplicas = 3
			finalDesired := desired.DeepCopy()
			finalDesired.Spec.UpdateStrategy.RollingUpdate = nil
			require.NoError(t, patchStatefulSet(context.Background(), updated2, finalDesired, "test", false, nil, client))
			updated3, err := client.AppsV1().StatefulSets("test").Get(context.Background(), "redis", metav1.GetOptions{})
			require.NoError(t, err)
			require.Nil(t, updated3.Spec.UpdateStrategy.RollingUpdate)
		})
	}
}

func TestEphemeralRolloutGuardBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                                                     string
		persistent, held, recreate, pending, template, wantError bool
		updatedReplicas, currentReplicas                         int32
		wantPartition                                            *int32
	}{
		{name: "metadata only"},
		{name: "persistent unchanged policy", persistent: true, template: true},
		{name: "held template", held: true, template: true},
		{name: "rolling template starts at highest ordinal", template: true, wantPartition: ptr.To(int32(2))},
		{name: "held recreation still unsafe", held: true, recreate: true, wantError: true},
		{name: "pending revision with 1 updated replica steps partition to 1", pending: true, updatedReplicas: 1, currentReplicas: 2, wantPartition: ptr.To(int32(1))},
		{name: "pending revision with all replicas updated clears partition", pending: true, updatedReplicas: 3, currentReplicas: 0, wantPartition: nil},
		{name: "keeping pending revision held", pending: true, held: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicas := int32(3)
			stored := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Generation:      1,
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: "RedisReplication", UID: "owner"}},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 1,
					Replicas:           3,
					ReadyReplicas:      3,
					UpdatedReplicas:    tc.updatedReplicas,
					CurrentReplicas:    tc.currentReplicas,
				},
			}
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
				if tc.wantPartition == nil {
					require.Nil(t, desired.Spec.UpdateStrategy.RollingUpdate)
				} else {
					require.NotNil(t, desired.Spec.UpdateStrategy.RollingUpdate)
					require.NotNil(t, desired.Spec.UpdateStrategy.RollingUpdate.Partition)
					require.Equal(t, *tc.wantPartition, *desired.Spec.UpdateStrategy.RollingUpdate.Partition)
				}
			}
		})
	}
}

func TestEphemeralGuardDoesNotTrustUnrelatedClaims(t *testing.T) {
	replicas := int32(3)
	stored := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: "RedisReplication", UID: "owner"}}},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	stored.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "config"}}}
	stored.Spec.Template.Spec.Containers = []corev1.Container{{Name: "redis", VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/etc/redis"}}}}
	desired := stored.DeepCopy()
	err := guardEphemeralRollout(stored, desired, []byte(`{"spec":{"template":{"metadata":{"annotations":{"changed":"yes"}}}}}`), false)
	require.NoError(t, err)
	require.NotNil(t, desired.Spec.UpdateStrategy.RollingUpdate)
	require.Equal(t, int32(2), *desired.Spec.UpdateStrategy.RollingUpdate.Partition)
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
			require.Equal(t, tc.want, persistentRedisStateMount(sts) != "")
		})
	}
}

func TestRolloutGuardRequiresSamePersistentState(t *testing.T) {
	for _, change := range []string{"claim", "subpath", "expression"} {
		t.Run(change, func(t *testing.T) {
			stored := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "redis", OwnerReferences: []metav1.OwnerReference{{APIVersion: "redis.redis.opstreelabs.in/v1beta2", Kind: "RedisReplication", UID: "owner"}}}}
			stored.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "original"}}}}
			stored.Spec.Template.Spec.Containers = []corev1.Container{{Name: "redis", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}}}
			desired := stored.DeepCopy()
			switch change {
			case "claim":
				desired.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "empty-replacement"
			case "subpath":
				desired.Spec.Template.Spec.Containers[0].VolumeMounts[0].SubPath = "empty-replacement"
			default:
				desired.Spec.Template.Spec.Containers[0].VolumeMounts[0].SubPathExpr = "$(STATE_DIR)"
			}
			changes := []byte(`{"spec":{"template":{"spec":{"volumes":[]}}}}`)
			require.ErrorContains(t, guardEphemeralRollout(stored, desired, changes, false), "OnDelete")
			desired.Spec.UpdateStrategy.Type = appsv1.OnDeleteStatefulSetStrategyType
			require.NoError(t, guardEphemeralRollout(stored, desired, changes, false))
		})
	}
}
