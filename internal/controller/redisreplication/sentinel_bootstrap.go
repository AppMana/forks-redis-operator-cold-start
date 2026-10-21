package redisreplication

import (
	"context"
	"fmt"
	"net"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type bootstrapMonitor struct {
	host, port string
	monitored  bool
	healthy    bool
}

// bootstrapPlaceholder selects only an explicit unspecified-address monitor.
// It never authorizes Redis promotion or replacement of an established monitor.
func bootstrapPlaceholder(states []bootstrapMonitor, size int32, primary corev1.Pod) int {
	if size <= 0 || len(states) != int(size) {
		return -1
	}
	placeholder, initialized := -1, false
	for i, state := range states {
		if !state.monitored || state.port != "6379" {
			return -1
		}
		if ip := net.ParseIP(state.host); ip != nil && ip.IsUnspecified() {
			placeholder = i
			continue
		}
		if !state.healthy || podForAddress([]corev1.Pod{primary}, net.JoinHostPort(state.host, state.port)) != primary.Name {
			return -1
		}
		initialized = true
	}
	if !initialized {
		return -1 // Ordinary first bootstrap handles the all-unconfigured case.
	}
	return placeholder
}

// initializeEmbeddedSentinelPlaceholder resumes partial bootstrap before the
// majority gate. All real monitors must agree with a complete, lineage-checked
// Redis topology; only one placeholder is initialized, then authority is observed
// again next reconcile. A crash needs no in-memory progress or weaker quorum.
func (r *Reconciler) initializeEmbeddedSentinelPlaceholder(ctx context.Context, inst *rrvb2.RedisReplication, topology k8sutils.RedisReplicationTopology) (bool, error) {
	if !inst.EnableSentinel() || !topology.Complete() || len(topology.Masters) != 1 || inst.Spec.Size == nil || topology.Observed() != int(*inst.Spec.Size) || r.Checker == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	groups, err := listSentinelGroups(ctx, r.Client, inst)
	if err != nil {
		return false, err
	}
	if len(groups) != 1 { // Never bypass conflicting external Sentinel authority.
		return false, nil
	}
	primary, err := r.K8sClient.CoreV1().Pods(inst.Namespace).Get(ctx, topology.Masters[0], metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	pods := groups[0].pods
	password, err := r.sentinelPassword(ctx, inst)
	if err != nil {
		return false, err
	}
	redisClient := redis.NewClient()
	states := make([]bootstrapMonitor, len(pods))
	for i, pod := range pods {
		if !k8sutils.IsRedisPodProbeable(&pod) {
			return false, nil
		}
		svc := redisClient.Connect(&redis.ConnectionInfo{Host: pod.Status.PodIP, Port: "26379", Password: password})
		state := &states[i]
		state.host, state.port, state.monitored, err = svc.SentinelGetMasterAddrByName(ctx, masterGroupName)
		if err != nil {
			return false, err
		}
		state.healthy, err = svc.SentinelMonitorHealthy(ctx, masterGroupName)
		if err != nil {
			return false, err
		}
	}
	index := bootstrapPlaceholder(states, inst.Spec.Sentinel.Size, *primary)
	if index < 0 {
		return false, nil
	}
	allPods := append(append([]string{}, topology.Masters...), topology.Slaves...)
	counts := r.Checker.GetReplicationKeyCounts(ctx, inst, allPods)
	if err := k8sutils.ValidateRecoveryLineage(ctx, r.K8sClient, inst, allPods, primary.Name, counts); err != nil {
		return false, fmt.Errorf("placeholder bootstrap data authority: %w", err)
	}
	// Recheck the specific placeholder immediately before its initialization.
	svc := redisClient.Connect(&redis.ConnectionInfo{Host: pods[index].Status.PodIP, Port: "26379", Password: password})
	host, port, monitored, err := svc.SentinelGetMasterAddrByName(ctx, masterGroupName)
	if err != nil {
		return false, err
	}
	if ip := net.ParseIP(host); !monitored || ip == nil || !ip.IsUnspecified() || port != "6379" {
		return false, nil
	}
	masterPassword, err := r.masterPassword(ctx, inst)
	if err != nil {
		return false, err
	}
	address := primary.Status.PodIP
	if inst.Spec.Sentinel.ResolveHostnames == "yes" {
		address = replicationPodHostname(inst, primary.Name)
	}
	if err := r.configureSentinelPod(ctx, redisClient, inst, pods[index], address, masterPassword); err != nil {
		return false, err
	}
	return true, nil
}
