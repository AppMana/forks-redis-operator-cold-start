package k8sutils

import (
	"fmt"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type annotatedObject interface {
	metav1.Object
	runtime.Object
}

// setDesiredLastApplied records obj as the last-applied configuration that the
// next three-way patch compares against. Call it before annotations preserved
// from the live object are merged into obj: whatever the record holds and the
// next desired object lacks is patched away, so preserved annotations recorded
// here would be deleted and merged back on every reconcile. Server-populated
// metadata is left out so that an unchanged desired object records the same
// bytes.
func setDesiredLastApplied(obj annotatedObject) error {
	desired, ok := obj.DeepCopyObject().(annotatedObject)
	if !ok {
		return fmt.Errorf("%T does not expose object metadata", obj)
	}
	desired.SetResourceVersion("")
	desired.SetManagedFields(nil)
	desired.SetCreationTimestamp(metav1.Time{})
	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(desired); err != nil {
		return err
	}
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[patch.LastAppliedConfig] = desired.GetAnnotations()[patch.LastAppliedConfig]
	obj.SetAnnotations(annotations)
	return nil
}
