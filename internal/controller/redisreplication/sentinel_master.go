package redisreplication

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// sentinelQueryTimeout bounds each Sentinel query so that an unreachable
// Sentinel cannot hold up role publication for the others.
const sentinelQueryTimeout = 2 * time.Second

// sentinelGroup is one Sentinel deployment that monitors a RedisReplication:
// the embedded Sentinel of spec.sentinel, or a RedisSentinel whose
// redisReplicationName names it.
type sentinelGroup struct {
	masterGroup string
	size        int32
	pods        []corev1.Pod
	password    *commonapi.ExistingPasswordSecret
	tls         *commonapi.TLSConfig
}

func listSentinelGroups(ctx context.Context, r client.Reader, rr *rrvb2.RedisReplication) ([]sentinelGroup, error) {
	var groups []sentinelGroup
	if rr.EnableSentinel() {
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(rr.Namespace), client.MatchingLabels(
			common.GetRedisLabels(rr.SentinelStatefulSet(), common.SetupTypeSentinel, "sentinel", nil),
		)); err != nil {
			return nil, err
		}
		groups = append(groups, sentinelGroup{
			masterGroup: masterGroupName,
			size:        rr.Spec.Sentinel.Size,
			pods:        pods.Items,
			password:    rr.Spec.Sentinel.ExistingPasswordSecret,
		})
	}

	var sentinels rsvb2.RedisSentinelList
	if err := r.List(ctx, &sentinels, client.InNamespace(rr.Namespace)); err != nil {
		return nil, err
	}
	for _, rs := range sentinels.Items {
		config := rs.Spec.RedisSentinelConfig
		if config == nil || config.RedisReplicationName != rr.Name || rs.Spec.Size == nil {
			continue
		}
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(rs.Namespace), client.MatchingLabels(
			common.GetRedisLabels(rs.GetStatefulSetName(), common.SetupTypeSentinel, "sentinel", nil),
		)); err != nil {
			return nil, err
		}
		groups = append(groups, sentinelGroup{
			masterGroup: config.MasterGroupName,
			size:        *rs.Spec.Size,
			pods:        pods.Items,
			password:    rs.Spec.KubernetesConfig.ExistingPasswordSecret,
			tls:         rs.Spec.TLS,
		})
	}
	return groups, nil
}

// sentinelElectedMaster asks every Sentinel that monitors rr which address it
// currently holds as the master, and maps the address a majority of each
// deployment agrees on to a Redis pod.
//
// ok is false when no Sentinel monitors rr, or when no deployment produced a
// majority answer, or when deployments disagree; callers then fall back to
// what the Redis pods themselves report. ok is true with pod=="" when the
// Sentinels agree on an address that no live Redis pod holds, which is the
// case between a primary dying and Sentinel promoting a replica.
func (r *Reconciler) sentinelElectedMaster(ctx context.Context, rr *rrvb2.RedisReplication) (string, bool) {
	if r.SentinelElectedMaster != nil {
		return r.SentinelElectedMaster(ctx, rr)
	}
	logger := log.FromContext(ctx)
	groups, err := listSentinelGroups(ctx, r.Client, rr)
	if err != nil {
		logger.Error(err, "list Sentinels monitoring replication")
		return "", false
	}
	if len(groups) == 0 {
		return "", false
	}
	checker := redishealer.NewChecker(r.K8sClient)
	redisClient := redis.NewClient()
	elected := ""
	for _, group := range groups {
		password, err := checker.GetPassword(ctx, rr.Namespace, group.password)
		if err != nil {
			logger.Error(err, "get Sentinel password", "masterGroup", group.masterGroup)
			continue
		}
		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			answers []string
		)
		for _, pod := range group.pods {
			if pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
				continue
			}
			connInfo := redishealer.ConnectionInfoForPod(ctx, pod, password, group.tls, r.K8sClient, rr.Namespace, "26379")
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				queryCtx, cancel := context.WithTimeout(ctx, sentinelQueryTimeout)
				defer cancel()
				host, port, monitored, err := redisClient.Connect(connInfo).SentinelGetMasterAddrByName(queryCtx, group.masterGroup)
				if err != nil {
					logger.V(1).Info("Sentinel did not answer", "sentinel", name, "error", err.Error())
					return
				}
				if monitored {
					mu.Lock()
					answers = append(answers, net.JoinHostPort(host, port))
					mu.Unlock()
				}
			}(pod.Name)
		}
		wg.Wait()
		addr, ok := majorityAddress(answers, group.size)
		if !ok {
			continue
		}
		if elected != "" && elected != addr {
			logger.Info("Sentinel deployments disagree on the master", "first", elected, "second", addr)
			return "", false
		}
		elected = addr
	}
	if elected == "" {
		return "", false
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(rr.Namespace), client.MatchingLabels(
		common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil),
	)); err != nil {
		logger.Error(err, "list Redis pods")
		return "", false
	}
	return podForAddress(pods.Items, elected), true
}

// majorityAddress returns the address that more than half of a Sentinel
// deployment of the given size reports. Counting against the configured size
// rather than the Sentinels that answered keeps a minority of stale Sentinels
// from naming the master.
func majorityAddress(answers []string, size int32) (string, bool) {
	if size <= 0 {
		return "", false
	}
	votes := map[string]int32{}
	for _, addr := range answers {
		votes[addr]++
		if votes[addr] > size/2 {
			return addr, true
		}
	}
	return "", false
}

// podForAddress maps a Sentinel master address (pod IP, or pod FQDN with
// resolve-hostnames) to the live Redis pod that holds it.
func podForAddress(pods []corev1.Pod, addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port != "6379" {
		return ""
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" {
			continue
		}
		fqdn := fmt.Sprintf("%s.%s.%s.svc.%s", pod.Name, common.GetHeadlessServiceNameFromPodName(pod.Name), pod.Namespace, envs.GetServiceDNSDomain())
		if pod.Status.PodIP == host || strings.TrimSuffix(host, ".") == fqdn {
			return pod.Name
		}
	}
	return ""
}
