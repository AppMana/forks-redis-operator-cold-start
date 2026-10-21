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
// ok is false only when no Sentinel group is configured or every group is
// positively unconfigured during bootstrap. ok is true with pod=="" for
// unresolved authority (unreachable/minority/conflicting groups, or a real
// elected address without a live pod). That state must block fallback repair.
func (r *Reconciler) sentinelElectedMaster(ctx context.Context, rr *rrvb2.RedisReplication) (string, bool) {
	if r.SentinelElectedMaster != nil {
		return r.SentinelElectedMaster(ctx, rr)
	}
	logger := log.FromContext(ctx)
	groups, err := listSentinelGroups(ctx, r.Client, rr)
	if err != nil {
		logger.Error(err, "list Sentinels monitoring replication")
		return "", true
	}
	if len(groups) == 0 {
		return "", false
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(rr.Namespace), client.MatchingLabels(
		common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil),
	)); err != nil {
		logger.Error(err, "list Redis pods")
		return "", true
	}
	checker := redishealer.NewChecker(r.K8sClient)
	redisClient := redis.NewClient()
	elected := ""
	for _, group := range groups {
		// Before any Sentinel pods exist, initial Redis bootstrap must be able
		// to create the primary they will monitor. Never use this exception for
		// an already recorded deployment whose Sentinel pods disappeared.
		if len(group.pods) == 0 && rr.Status.MasterNode == "" {
			continue
		}
		password, err := checker.GetPassword(ctx, rr.Namespace, group.password)
		if err != nil {
			logger.Error(err, "get Sentinel password", "masterGroup", group.masterGroup)
			return "", true
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
				answer := ""
				if monitored {
					answer = canonicalSentinelAddress(pods.Items, net.JoinHostPort(host, port))
				}
				mu.Lock()
				answers = append(answers, answer)
				mu.Unlock()
			}(pod.Name)
		}
		wg.Wait()
		// Include surplus pods during replacement/downscale in the electorate,
		// including unreachable or terminating members. Otherwise a configured
		// size smaller than the live inventory can admit competing majorities.
		electorate := max(group.size, int32(len(group.pods)))
		addr, ok := majorityAddress(answers, electorate)
		if !ok {
			if sentinelsUnconfigured(answers, electorate) {
				continue
			}
			logger.Info("Sentinel authority unresolved; refusing fallback election", "masterGroup", group.masterGroup)
			return "", true
		}
		if elected != "" && elected != addr {
			logger.Info("Sentinel deployments disagree on the master", "first", elected, "second", addr)
			return "", true
		}
		elected = addr
	}
	if elected == "" {
		return "", false
	}

	return podForAddress(pods.Items, elected), true
}

func sentinelsUnconfigured(answers []string, size int32) bool {
	if size <= 0 || len(answers) < int(size) {
		return false
	}
	for _, addr := range answers {
		if addr == "" {
			continue // Successful lookup explicitly reported no monitor.
		}
		host, _, err := net.SplitHostPort(addr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsUnspecified() {
			return false
		}
	}
	return true
}

func canonicalSentinelAddress(pods []corev1.Pod, addr string) string {
	// Normalize only exact aliases validated against the current namespace's
	// live pods. Never resolve arbitrary DNS or conflate different ports.
	name := podForAddress(pods, addr)
	if name != "" {
		for _, pod := range pods {
			if pod.Name == name {
				return net.JoinHostPort(pod.Status.PodIP, "6379")
			}
		}
	}
	return addr
}

// majorityAddress returns the address that more than half of a Sentinel
// deployment of the given size reports. Counting against the configured size
// rather than the Sentinels that answered keeps a minority of stale Sentinels
// from naming the master.
func majorityAddress(answers []string, size int32) (string, bool) {
	if size <= 0 {
		return "", false
	}
	// Callers include nonresponding pods in size. Still account for every
	// supplied observation if the inventory exceeded the configured size.
	size = max(size, int32(len(answers)))
	votes := map[string]int32{}
	for _, addr := range answers {
		if addr == "" {
			continue
		}
		// Init-generated Sentinel configs monitor an unspecified address until
		// Redis bootstraps. This is not an election for a missing/dead primary:
		// treating it as authoritative deadlocks first installation. Keep the
		// configured majority denominator, and retain every real address.
		host, _, err := net.SplitHostPort(addr)
		if err == nil {
			if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
				continue
			}
		}
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
