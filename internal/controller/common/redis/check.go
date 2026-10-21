package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	rr "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	agentutil "github.com/OT-CONTAINER-KIT/redis-operator/internal/agent/util"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Checker interface {
	GetMasterFromReplication(ctx context.Context, rr *rr.RedisReplication) (corev1.Pod, error)
	GetReplicationKeyCounts(ctx context.Context, rr *rr.RedisReplication, podNames []string) map[string]int64
	GetPassword(ctx context.Context, ns string, secret *commonapi.ExistingPasswordSecret) (string, error)
	CheckClusterSlotsAssigned(ctx context.Context, cr *rcvb2.RedisCluster) (bool, error)
}

type checker struct {
	redis redis.Client
	k8s   kubernetes.Interface
}

func NewChecker(clientset kubernetes.Interface) Checker {
	return &checker{
		k8s:   clientset,
		redis: redis.NewClient(),
	}
}

func (c *checker) GetPassword(ctx context.Context, ns string, secret *commonapi.ExistingPasswordSecret) (string, error) {
	if secret == nil {
		return "", nil
	}
	if secret.Name == nil || secret.Key == nil || *secret.Name == "" || *secret.Key == "" {
		return "", errors.New("password secret reference requires a non-empty name and key")
	}
	secretName, err := c.k8s.CoreV1().Secrets(ns).Get(ctx, *secret.Name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for key, value := range secretName.Data {
		if key == *secret.Key {
			if features.Enabled(features.GenerateConfigInInitContainer) {
				return agentutil.SanitizeConfigValue(string(value)), nil
			}
			return strings.TrimSpace(string(value)), nil
		}
	}
	return "", errors.New("secret key not found")
}

func (c *checker) GetMasterFromReplication(ctx context.Context, rr *rr.RedisReplication) (corev1.Pod, error) {
	sts, err := c.k8s.AppsV1().StatefulSets(rr.Namespace).Get(ctx, rr.GetStatefulSetName(), metav1.GetOptions{})
	if err != nil {
		return corev1.Pod{}, err
	}

	var labels []string
	for k, v := range sts.Spec.Selector.MatchLabels {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	pods, err := c.k8s.CoreV1().Pods(rr.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: strings.Join(labels, ","),
	})
	if err != nil {
		return corev1.Pod{}, err
	}

	password, err := c.GetPassword(ctx, rr.Namespace, rr.Spec.KubernetesConfig.ExistingPasswordSecret)
	if err != nil {
		return corev1.Pod{}, err
	}

	var masterPods []corev1.Pod
	var unknownRoles []error
	if sts.Spec.Replicas == nil || len(pods.Items) < int(*sts.Spec.Replicas) {
		return corev1.Pod{}, errors.New("incomplete Redis pod inventory; preserving Sentinel monitor")
	}
	for _, pod := range pods.Items {
		if !k8sutils.IsRedisPodProbeable(&pod) || pod.DeletionTimestamp != nil {
			unknownRoles = append(unknownRoles, fmt.Errorf("redis role unavailable for pod %s", pod.Name))
			continue
		}
		connInfo := createConnectionInfo(ctx, pod, password, rr.Spec.TLS, c.k8s, rr.Namespace, "6379")
		isMaster, err := c.redis.Connect(connInfo).IsMaster(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return corev1.Pod{}, ctxErr
			}
			unknownRoles = append(unknownRoles, fmt.Errorf("inspect Redis role for pod %s: %w", pod.Name, err))
			continue
		}
		if isMaster {
			masterPods = append(masterPods, pod)
		}
	}
	// Unreachable or terminating nodes may include the newly elected master.
	// Existing Sentinel failover remains autonomous; do not replace its monitor
	// using a partial Redis view. The replication controller repairs topology.
	if len(unknownRoles) != 0 {
		return corev1.Pod{}, fmt.Errorf("incomplete Redis role observation; preserving Sentinel monitor: %w", errors.Join(unknownRoles...))
	}

	// Replica links can lag behind a Sentinel promotion. Picking the first
	// master with attached replicas could re-MONITOR the old primary, erasing
	// Sentinel's newer election. Let the replication reconciler resolve roles
	// using Sentinel authority instead of making another election here.
	if len(masterPods) > 1 {
		return corev1.Pod{}, errors.New("multiple Redis masters observed; refusing to select a Sentinel monitor target by replica count")
	}
	var realMasterPod corev1.Pod
	for _, pod := range masterPods {
		connInfo := createConnectionInfo(ctx, pod, password, rr.Spec.TLS, c.k8s, rr.Namespace, "6379")
		count, err := c.redis.Connect(connInfo).GetAttachedReplicaCount(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return corev1.Pod{}, ctxErr
			}
			continue
		}
		if count != 0 {
			realMasterPod = pod
			break
		} else {
			replicaNum := *sts.Spec.Replicas
			if replicaNum == 1 {
				realMasterPod = pod
				break
			}
		}
	}
	return realMasterPod, nil
}

func (c *checker) GetReplicationKeyCounts(ctx context.Context, rr *rr.RedisReplication, podNames []string) map[string]int64 {
	keyCounts := make(map[string]int64, len(podNames))
	password, err := c.GetPassword(ctx, rr.Namespace, rr.Spec.KubernetesConfig.ExistingPasswordSecret)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to get redis password, key counts unavailable")
		return keyCounts
	}
	for _, podName := range podNames {
		pod, err := c.k8s.CoreV1().Pods(rr.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to get pod, skipping key count", "pod", podName)
			continue
		}
		if !k8sutils.IsRedisPodProbeable(pod) {
			continue
		}
		connInfo := createConnectionInfo(ctx, *pod, password, rr.Spec.TLS, c.k8s, rr.Namespace, "6379")
		// Metadata/credential lookups can wait on the Kubernetes rate limiter.
		// Start the network probe budget only after those lookups complete.
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		keys, err := c.redis.Connect(connInfo).GetKeyCount(probeCtx)
		cancel()
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to get key count, skipping pod", "pod", podName)
			continue
		}
		keyCounts[podName] = keys
	}
	return keyCounts
}

// CheckClusterSlotsAssigned verifies if all Redis cluster slots (16384 total) are properly assigned
func (c *checker) CheckClusterSlotsAssigned(ctx context.Context, cr *rcvb2.RedisCluster) (bool, error) {
	leaderPodName := cr.Name + "-leader-0"
	pod, err := c.k8s.CoreV1().Pods(cr.Namespace).Get(ctx, leaderPodName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	password, err := c.GetPassword(ctx, cr.Namespace, cr.Spec.KubernetesConfig.ExistingPasswordSecret)
	if err != nil {
		return false, err
	}

	connInfo := createConnectionInfo(ctx, *pod, password, cr.Spec.TLS, c.k8s, cr.Namespace, "6379")

	clusterStatus, err := c.redis.Connect(connInfo).GetClusterInfo(ctx)
	if err != nil {
		return false, err
	}

	allAssigned := clusterStatus.SlotsAssigned == 16384

	return allAssigned, nil
}
