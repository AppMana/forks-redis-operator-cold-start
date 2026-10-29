package k8sutils

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	removedOperatorAnnotation = "redis.opstreelabs.in/recreate-statefulset"
	foreignAnnotation         = "kubectl.kubernetes.io/last-applied-configuration"
)

// The live object was written by an earlier reconcile whose desired state still
// carried removedOperatorAnnotation, and another writer has since added
// foreignAnnotation. Both are preserved on update; neither may make every later
// reconcile rewrite the object.
func seedPreviouslyApplied(t *testing.T, obj metav1.Object, ro runtime.Object) {
	t.Helper()
	obj.SetResourceVersion("")
	annotations := obj.GetAnnotations()
	annotations[removedOperatorAnnotation] = "true"
	obj.SetAnnotations(annotations)
	require.NoError(t, patch.DefaultAnnotator.SetLastAppliedAnnotation(ro))
	annotations = obj.GetAnnotations()
	annotations[foreignAnnotation] = `{"kind":"StatefulSet"}`
	obj.SetAnnotations(annotations)
	obj.SetResourceVersion("1")
	obj.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate}})
}

func countUpdates(cl *fake.Clientset) int {
	n := 0
	for _, action := range cl.Actions() {
		if action.GetVerb() == "update" {
			n++
		}
	}
	return n
}

func requireDesiredOnlyOriginal(t *testing.T, ro runtime.Object) {
	t.Helper()
	original, err := patch.DefaultAnnotator.GetOriginalConfiguration(ro)
	require.NoError(t, err)
	var applied struct {
		Metadata map[string]json.RawMessage `json:"metadata"`
	}
	require.NoError(t, json.Unmarshal(original, &applied))
	for _, field := range []string{"resourceVersion", "managedFields", "creationTimestamp"} {
		require.NotContains(t, applied.Metadata, field)
	}
	var annotations map[string]string
	require.NoError(t, json.Unmarshal(applied.Metadata["annotations"], &annotations))
	require.NotContains(t, annotations, foreignAnnotation)
	require.NotContains(t, annotations, removedOperatorAnnotation)
}

func TestPatchStatefulSetConvergesWithPreservedAnnotations(t *testing.T) {
	desired := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			TypeMeta:   generateMetaInformation("StatefulSet", "apps/v1"),
			ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test", Annotations: map[string]string{"redis.opstreelabs.in": "true"}},
			Spec: appsv1.StatefulSetSpec{
				UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
				Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis", Image: "redis:7"}}}},
			},
		}
	}
	live := desired()
	seedPreviouslyApplied(t, live, live)
	cl := fake.NewSimpleClientset(live)

	for range 4 {
		stored, err := cl.AppsV1().StatefulSets("test").Get(context.Background(), "redis", metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, patchStatefulSet(context.Background(), stored, desired(), "test", false, nil, cl))
	}

	require.LessOrEqual(t, countUpdates(cl), 1)
	stored, err := cl.AppsV1().StatefulSets("test").Get(context.Background(), "redis", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, stored.Annotations, foreignAnnotation)
	requireDesiredOnlyOriginal(t, stored)
}

func TestPatchServiceConvergesWithPreservedAnnotations(t *testing.T) {
	desired := func() *corev1.Service {
		return &corev1.Service{
			TypeMeta:   generateMetaInformation("Service", "v1"),
			ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test", Annotations: map[string]string{"redis.opstreelabs.in": "true"}},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: map[string]string{"app": "redis"},
				Ports:    []corev1.ServicePort{{Name: "redis-client", Port: 6379, TargetPort: intstr.FromInt32(6379), Protocol: corev1.ProtocolTCP}},
			},
		}
	}
	live := desired()
	live.Spec.ClusterIP = "10.0.0.10"
	seedPreviouslyApplied(t, live, live)
	cl := fake.NewSimpleClientset(live)

	for range 4 {
		stored, err := cl.CoreV1().Services("test").Get(context.Background(), "redis", metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, patchService(context.Background(), stored, desired(), "test", cl))
	}

	require.LessOrEqual(t, countUpdates(cl), 1)
	stored, err := cl.CoreV1().Services("test").Get(context.Background(), "redis", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, stored.Annotations, foreignAnnotation)
	requireDesiredOnlyOriginal(t, stored)
}

func TestPatchPodDisruptionBudgetConvergesWithPreservedAnnotations(t *testing.T) {
	desired := func() *policyv1.PodDisruptionBudget {
		minAvailable := intstr.FromInt32(2)
		return &policyv1.PodDisruptionBudget{
			TypeMeta:   generateMetaInformation("PodDisruptionBudget", "policy/v1"),
			ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test", Annotations: map[string]string{"redis.opstreelabs.in": "true"}},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvailable,
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			},
		}
	}
	live := desired()
	seedPreviouslyApplied(t, live, live)
	cl := fake.NewSimpleClientset(live)

	for range 4 {
		stored, err := cl.PolicyV1().PodDisruptionBudgets("test").Get(context.Background(), "redis", metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, patchPodDisruptionBudget(context.Background(), stored, desired(), "test", cl))
	}

	require.LessOrEqual(t, countUpdates(cl), 1)
	stored, err := cl.PolicyV1().PodDisruptionBudgets("test").Get(context.Background(), "redis", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, stored.Annotations, foreignAnnotation)
	requireDesiredOnlyOriginal(t, stored)
}
