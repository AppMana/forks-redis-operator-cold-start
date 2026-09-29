package redissentinel

import (
	"context"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSentinelsForPod(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := rsvb2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	monitor := func(namespace, name, replication string) *rsvb2.RedisSentinel {
		return &rsvb2.RedisSentinel{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: rsvb2.RedisSentinelSpec{RedisSentinelConfig: &rsvb2.RedisSentinelConfig{RedisSentinelConfig: commonapi.RedisSentinelConfig{RedisReplicationName: replication}}}}
	}
	r := &RedisSentinelReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(monitor("appmana", "harbor", "redis-harbor"), monitor("appmana", "lago", "redis-lago"), monitor("other", "harbor", "redis-harbor")).Build()}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "appmana", Name: "redis-harbor-0", Labels: map[string]string{"app": "redis-harbor", "redis_setup_type": "replication"}}}
	requests := r.sentinelsForPod(context.Background(), pod)
	if len(requests) != 1 || requests[0].Namespace != "appmana" || requests[0].Name != "harbor" {
		t.Fatalf("unexpected monitors: %v", requests)
	}
	pod.Labels["redis_setup_type"] = "sentinel"
	if requests = r.sentinelsForPod(context.Background(), pod); len(requests) != 0 {
		t.Fatalf("sentinel pod must not trigger monitor repair: %v", requests)
	}
}
