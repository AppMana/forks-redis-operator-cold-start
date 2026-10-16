package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type roleProbe struct {
	master map[string]bool
	down   map[string]bool
}

func (p roleProbe) Connect(info *redis.ConnectionInfo) redis.Service {
	return roleProbeService{probe: p, host: info.Host}
}

type roleProbeService struct {
	redis.Service
	probe roleProbe
	host  string
}

func (s roleProbeService) IsMaster(context.Context) (bool, error) {
	if s.probe.down[s.host] {
		return false, errors.New("dial tcp " + s.host + ":6379: i/o timeout")
	}
	return s.probe.master[s.host], nil
}

func replicationPod(name, ip, role string, deleting bool) *corev1.Pod {
	labels := map[string]string{"app": "redis-repl", "redis_setup_type": "replication", "role": "replication"}
	if role != "" {
		labels[common.RedisRoleLabelKey] = role
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", Labels: labels},
		Status:     corev1.PodStatus{PodIP: ip},
	}
	if deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		pod.Finalizers = []string{"test/keep"}
	}
	return pod
}

func roleLabels(t *testing.T, k8s *fake.Clientset) map[string]string {
	t.Helper()
	pods, err := k8s.CoreV1().Pods("team-a").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	roles := map[string]string{}
	for _, pod := range pods.Items {
		roles[pod.Name] = pod.Labels[common.RedisRoleLabelKey]
	}
	return roles
}

// A drain of the primary's node: Sentinel promotes redis-repl-2 within seconds
// while redis-repl-1 (the old master) is terminating and then comes back empty
// as a second master. The writable Service must select exactly the elected
// master, and the terminating old master must leave it immediately.
func TestPublishReplicationRoleLabels(t *testing.T) {
	selector := map[string]string{"app": "redis-repl", "redis_setup_type": "replication", "role": "replication"}
	for _, tc := range []struct {
		name   string
		pods   []*corev1.Pod
		probe  roleProbe
		master string
		want   map[string]string
	}{
		{
			name: "terminating old master leaves the master service before failover",
			pods: []*corev1.Pod{
				replicationPod("redis-repl-0", "10.0.0.10", "slave", false),
				replicationPod("redis-repl-1", "10.0.0.11", "master", true),
				replicationPod("redis-repl-2", "10.0.0.12", "slave", false),
			},
			probe: roleProbe{master: map[string]bool{"10.0.0.11": true}},
			want:  map[string]string{"redis-repl-0": "slave", "redis-repl-1": "", "redis-repl-2": "slave"},
		},
		{
			name: "elected master moves while an empty replacement also reports master",
			pods: []*corev1.Pod{
				replicationPod("redis-repl-0", "10.0.0.10", "slave", false),
				replicationPod("redis-repl-1", "10.0.0.21", "master", false),
				replicationPod("redis-repl-2", "10.0.0.12", "slave", false),
			},
			probe:  roleProbe{master: map[string]bool{"10.0.0.21": true, "10.0.0.12": true}},
			master: "redis-repl-2",
			want:   map[string]string{"redis-repl-0": "slave", "redis-repl-1": "", "redis-repl-2": "master"},
		},
		{
			name: "elected master moves while the old master is pending replacement",
			pods: []*corev1.Pod{
				replicationPod("redis-repl-0", "10.0.0.10", "slave", false),
				replicationPod("redis-repl-1", "", "master", false),
				replicationPod("redis-repl-2", "10.0.0.12", "slave", false),
			},
			probe:  roleProbe{master: map[string]bool{"10.0.0.12": true}},
			master: "redis-repl-2",
			want:   map[string]string{"redis-repl-0": "slave", "redis-repl-1": "", "redis-repl-2": "master"},
		},
		{
			name: "unreachable old master leaves the master service once another pod is elected",
			pods: []*corev1.Pod{
				replicationPod("redis-repl-0", "10.0.0.10", "slave", false),
				replicationPod("redis-repl-1", "10.0.0.11", "master", false),
				replicationPod("redis-repl-2", "10.0.0.12", "slave", false),
			},
			probe:  roleProbe{master: map[string]bool{"10.0.0.12": true}, down: map[string]bool{"10.0.0.11": true}},
			master: "redis-repl-2",
			want:   map[string]string{"redis-repl-0": "slave", "redis-repl-1": "", "redis-repl-2": "master"},
		},
		{
			name: "unknown master never withdraws a reachable or unprobeable master",
			pods: []*corev1.Pod{
				replicationPod("redis-repl-0", "10.0.0.10", "master", false),
				replicationPod("redis-repl-1", "10.0.0.11", "slave", false),
				replicationPod("redis-repl-2", "10.0.0.12", "slave", false),
			},
			probe: roleProbe{master: map[string]bool{"10.0.0.10": true}, down: map[string]bool{"10.0.0.10": true}},
			want:  map[string]string{"redis-repl-0": "master", "redis-repl-1": "slave", "redis-repl-2": "slave"},
		},
		{
			name: "elected pod that does not report master is not published",
			pods: []*corev1.Pod{
				replicationPod("redis-repl-0", "10.0.0.10", "slave", false),
				replicationPod("redis-repl-1", "10.0.0.11", "slave", false),
				replicationPod("redis-repl-2", "10.0.0.12", "slave", false),
			},
			probe:  roleProbe{},
			master: "redis-repl-2",
			want:   map[string]string{"redis-repl-0": "slave", "redis-repl-1": "slave", "redis-repl-2": "slave"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k8s := fake.NewSimpleClientset()
			for _, pod := range tc.pods {
				_, err := k8s.CoreV1().Pods("team-a").Create(context.Background(), pod, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			h := &healer{k8s: k8s, redis: tc.probe}
			require.NoError(t, h.PublishReplicationRoleLabels(context.Background(), "team-a", selector, nil, nil, tc.master))
			require.Equal(t, tc.want, roleLabels(t, k8s))
		})
	}
}
