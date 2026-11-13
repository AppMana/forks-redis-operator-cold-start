package redisreplication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// sentinelQueryTimeout bounds each Sentinel query so that an unreachable
// Sentinel cannot hold up role publication for the others.
const sentinelQueryTimeout = 2 * time.Second

// RecoverySentinelOwnerLabel associates additional voters with a managed
// Sentinel StatefulSet for read-only election observation, not lifecycle control.
const RecoverySentinelOwnerLabel = "redis.redis.opstreelabs.in/sentinel-owner"

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
		additional, err := additionalSentinelPods(ctx, r, rr.Namespace, rr.SentinelStatefulSet(), &pods)
		if err != nil {
			return nil, err
		}
		groups = append(groups, sentinelGroup{
			masterGroup: masterGroupName,
			size:        rr.Spec.Sentinel.Size + additional,
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
		additional, err := additionalSentinelPods(ctx, r, rs.Namespace, rs.GetStatefulSetName(), &pods)
		if err != nil {
			return nil, err
		}
		groups = append(groups, sentinelGroup{
			masterGroup: config.MasterGroupName,
			size:        *rs.Spec.Size + additional,
			pods:        pods.Items,
			password:    rs.Spec.KubernetesConfig.ExistingPasswordSecret,
			tls:         rs.Spec.TLS,
		})
	}
	return groups, nil
}

func additionalSentinelPods(ctx context.Context, reader client.Reader, namespace, owner string, pods *corev1.PodList) (int32, error) {
	var additional corev1.PodList
	if err := reader.List(ctx, &additional, client.InNamespace(namespace), client.MatchingLabels{RecoverySentinelOwnerLabel: owner}); err != nil {
		return 0, err
	}
	seen := map[string]bool{}
	for _, pod := range pods.Items {
		seen[pod.Name] = true
	}
	var count int32
	for _, pod := range additional.Items {
		if !seen[pod.Name] {
			pods.Items = append(pods.Items, pod)
			seen[pod.Name] = true
			count++
		}
	}
	return count, nil
}

// sentinelElectedMaster asks every Sentinel that monitors rr which address it
// currently holds as the master, and maps the address a majority of each
// deployment agrees on to a Redis pod.
//
// ok is false only when no Sentinel group is configured or every group is
// positively unconfigured during bootstrap. ok is true with pod=="" for
// unresolved authority (unreachable/minority/conflicting groups, or a real
// elected address without a live pod). That state must block fallback repair.
//
// A group without a majority still names its master when every Sentinel in it
// answered and each one either monitors that same address or holds no
// election at all (see uncontestedSentinelAddress), provided the Redis data
// proves that following it discards nothing (see verifySoleDataMaster).
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
	var namespacePods corev1.PodList
	_ = r.List(ctx, &namespacePods, client.InNamespace(rr.Namespace))
	liveHosts := liveNamespaceHosts(namespacePods.Items)
	checker := redishealer.NewChecker(r.K8sClient)
	redisClient := r.SentinelClient
	if redisClient == nil {
		redisClient = redis.NewClient()
	}
	elected := ""
	// uncontested is set when a group named its master without a majority,
	// which is only accepted together with proof from the Redis data itself.
	uncontested := false
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
			mu         sync.Mutex
			wg         sync.WaitGroup
			answers    []string
			electorate = max(group.size, int32(len(group.pods)))
			identities = map[string]bool{}
			duplicate  bool
		)
		for _, pod := range group.pods {
			if pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
				continue
			}
			connInfo := redishealer.ConnectionInfoForPod(ctx, pod, password, group.tls, r.K8sClient, rr.Namespace, "26379")
			wg.Add(1)
			go func(name, selfIP string) {
				defer wg.Done()
				queryCtx, cancel := context.WithTimeout(ctx, sentinelQueryTimeout)
				defer cancel()
				service := redisClient.Connect(connInfo)
				identity, err := service.SentinelMyID(queryCtx)
				if err != nil || identity == "" {
					return
				}
				mu.Lock()
				if identities[identity] {
					duplicate = true
				}
				identities[identity] = true
				mu.Unlock()
				host, port, monitored, err := service.SentinelGetMasterAddrByName(queryCtx, group.masterGroup)
				if err != nil {
					logger.V(1).Info("Sentinel did not answer", "sentinel", name, "error", err.Error())
					return
				}
				answer := ""
				known := int32(0)
				if monitored {
					info, err := service.GetInfoSentinel(queryCtx)
					if err != nil {
						return // Unknown membership cannot contribute an election vote.
					}
					master, found := info.Master(group.masterGroup)
					if !found || master.Sentinels < 1 {
						return
					}
					known = int32(master.Sentinels)
					if peers, peerErr := service.SentinelPeers(queryCtx, group.masterGroup); peerErr == nil && len(peers) > 0 {
						liveKnown := int32(1)
						seenPeerHosts := map[string]bool{selfIP: true}
						for _, peer := range peers {
							peerHost := strings.TrimSuffix(peer.IP, ".")
							if peerHost == "" || seenPeerHosts[peerHost] {
								continue
							}
							if liveHosts[peerHost] {
								seenPeerHosts[peerHost] = true
								liveKnown++
							}
						}
						known = liveKnown
					}
					answer = canonicalSentinelAddress(pods.Items, net.JoinHostPort(host, port))
				}
				mu.Lock()
				answers = append(answers, answer)
				electorate = max(electorate, known)
				mu.Unlock()
			}(pod.Name, pod.Status.PodIP)
		}
		wg.Wait()
		if duplicate {
			logger.Info("Duplicate Sentinel identities; refusing fallback election", "masterGroup", group.masterGroup)
			return "", true
		}
		// Include surplus pods during replacement/downscale in the electorate,
		// including unreachable or terminating members. Otherwise a configured
		// size smaller than the live inventory can admit competing majorities.
		addr, ok := majorityAddress(answers, electorate)
		if !ok {
			if sentinelsUnconfigured(answers, electorate) {
				continue
			}
			named, ok := uncontestedSentinelAddress(answers, electorate)
			if !ok {
				logger.Info("Sentinel authority unresolved; refusing fallback election", "masterGroup", group.masterGroup)
				return "", true
			}
			addr = named
			uncontested = true
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

	pod := podForAddress(pods.Items, elected)
	if uncontested {
		if pod == "" {
			logger.Info("Sentinel authority unresolved; refusing fallback election", "reason", "the only monitored address belongs to no live Redis pod")
			return "", true
		}
		if err := r.verifySoleDataMaster(ctx, rr, pod); err != nil {
			logger.Info("Sentinel authority unresolved; refusing fallback election", "sentinelMaster", pod, "reason", err.Error())
			return "", true
		}
		logger.Info("Following the configured Sentinels: the others are unconfigured and the Redis data confirms the monitored master",
			"sentinelMaster", pod)
	}
	return pod, true
}

// uncontestedSentinelAddress returns the single real address monitored by the
// Sentinels of a deployment in which every member answered and every other
// member holds no election at all: it monitors nothing, or only the
// unspecified-address placeholder an init-generated config starts with.
//
// A placeholder Sentinel never observed a master, so it has no election that
// following its configured peers could override. A Sentinel that did not
// answer is different: it may hold a newer election, so one missing answer
// refuses. So do two different real addresses.
//
// The result is not a majority. Callers must additionally prove from the
// Redis data that following it cannot discard anything.
func uncontestedSentinelAddress(answers []string, electorate int32) (string, bool) {
	if electorate <= 0 || int32(len(answers)) < electorate {
		return "", false
	}
	named := ""
	for _, addr := range answers {
		if isUnspecifiedSentinelAddr(addr) {
			continue
		}
		if named != "" && named != addr {
			return "", false
		}
		named = addr
	}
	return named, named != ""
}

// verifySoleDataMaster proves that candidate holds the authoritative dataset
// before every other pod is made its replica. It returns nil only when
//   - every configured pod was observed, with its key count and replication
//     state (a missing observation is never treated as empty),
//   - candidate itself reports role:master,
//   - every other self-declared master is empty: no keys, no replication
//     offset and no attached replicas, and
//   - the recovery lineage rules accept candidate against every remaining
//     pod, so a replica holding data shares its replication ID and is not
//     ahead of it,
//
// or when candidate is the recorded master among unattached masters that share
// no replication history (see k8sutils.RecordedMasterAmongUnrelatedMasters):
// pods restarted from their own disks hold data too, and then the record and
// the Sentinels that monitor a master agree on the same pod.
func (r *Reconciler) verifySoleDataMaster(ctx context.Context, rr *rrvb2.RedisReplication, candidate string) error {
	if r.Checker == nil {
		return errors.New("no checker configured")
	}
	topology, err := r.redisReplicationTopology(ctx, rr)
	if err != nil {
		return fmt.Errorf("observe Redis roles: %w", err)
	}
	if !topology.Complete() || rr.Spec.Size == nil || topology.Observed() != int(*rr.Spec.Size) {
		return fmt.Errorf("incomplete Redis role observation, unobserved pods %v", topology.Unobserved)
	}
	if !slices.Contains(topology.Masters, candidate) {
		return fmt.Errorf("%s is not serving as master", candidate)
	}
	allPods := append(append([]string{}, topology.Masters...), topology.Slaves...)
	counts := r.Checker.GetReplicationKeyCounts(ctx, rr, allPods)
	infos := r.observeReplicationInfo(ctx, rr, allPods)
	for _, pod := range allPods {
		if _, ok := counts[pod]; !ok {
			return fmt.Errorf("key count of %s is unknown", pod)
		}
		if _, ok := infos[pod]; !ok {
			return fmt.Errorf("replication state of %s is unknown", pod)
		}
	}
	if recorded, ok := k8sutils.RecordedMasterAmongUnrelatedMasters(rr, topology.Masters, topology.Slaves, counts, infos); ok && recorded == candidate {
		return nil
	}
	for _, master := range topology.Masters {
		if master == candidate {
			continue
		}
		if info := infos[master]; counts[master] != 0 || info.EffectiveOffset() != 0 || info.ConnectedSlaves != 0 {
			return fmt.Errorf("master %s is not empty: %d keys, replication offset %d, %d attached replicas",
				master, counts[master], info.EffectiveOffset(), info.ConnectedSlaves)
		}
	}
	validate := r.ValidateRecoveryLineage
	if validate == nil {
		validate = k8sutils.ValidateRecoveryLineage
	}
	return validate(ctx, r.K8sClient, rr, allPods, candidate, counts)
}

// noSentinelNamesAnotherPod reports whether every Sentinel monitoring rr
// answered and none of them names a live Redis pod other than candidate. A
// Sentinel may monitor nothing, the unspecified-address placeholder, candidate
// itself, or an address that belongs to no live pod: none of those is an
// election for a pod that exists. A Sentinel that is missing or does not
// answer may hold one, so it refuses.
func (r *Reconciler) noSentinelNamesAnotherPod(ctx context.Context, rr *rrvb2.RedisReplication, candidate string) bool {
	if r.Client == nil {
		return true
	}
	groups, err := listSentinelGroups(ctx, r.Client, rr)
	if err != nil {
		return false
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(rr.Namespace), client.MatchingLabels(
		common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil),
	)); err != nil {
		return false
	}
	checker := redishealer.NewChecker(r.K8sClient)
	redisClient := r.SentinelClient
	if redisClient == nil {
		redisClient = redis.NewClient()
	}
	for _, group := range groups {
		if int32(len(group.pods)) < group.size {
			return false
		}
		password, err := checker.GetPassword(ctx, rr.Namespace, group.password)
		if err != nil {
			return false
		}
		for _, pod := range group.pods {
			if pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
				return false
			}
			connInfo := redishealer.ConnectionInfoForPod(ctx, pod, password, group.tls, r.K8sClient, rr.Namespace, "26379")
			queryCtx, cancel := context.WithTimeout(ctx, sentinelQueryTimeout)
			host, port, monitored, err := redisClient.Connect(connInfo).SentinelGetMasterAddrByName(queryCtx, group.masterGroup)
			cancel()
			if err != nil {
				return false
			}
			if !monitored {
				continue
			}
			addr := net.JoinHostPort(host, port)
			if isUnspecifiedSentinelAddr(addr) {
				continue
			}
			if named := podForAddress(pods.Items, addr); named != "" && named != candidate {
				return false
			}
		}
	}
	return true
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

func liveNamespaceHosts(pods []corev1.Pod) map[string]bool {
	hosts := make(map[string]bool, len(pods)*2)
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" {
			continue
		}
		hosts[pod.Status.PodIP] = true
		fqdn := fmt.Sprintf("%s.%s.%s.svc.%s", pod.Name, common.GetHeadlessServiceNameFromPodName(pod.Name), pod.Namespace, envs.GetServiceDNSDomain())
		hosts[fqdn] = true
	}
	return hosts
}

func isUnspecifiedSentinelAddr(addr string) bool {
	if addr == "" {
		return true
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// sentinelMonitoredMasterDeadOrStuck reports whether all configured Sentinel
// groups monitoring rr are reachable by a live majority yet stuck monitoring a
// dead pod IP (an IP that belongs to no live Redis pod in rr.Namespace) or
// unconfigured placeholder (0.0.0.0) while all configured Redis pods are live
// and observed. In that state, Sentinel cannot complete failover on its own
// (for example, when dead sentinel peer IPs or a restarted 0.0.0.0 sentinel
// prevent leader election, or when all replicas point at the dead master IP),
// so the operator must proceed with deterministic recovery and re-point Sentinel.
func (r *Reconciler) sentinelMonitoredMasterDeadOrStuck(ctx context.Context, rr *rrvb2.RedisReplication, topology k8sutils.RedisReplicationTopology) bool {
	if r.Client == nil || r.SentinelElectedMaster != nil {
		return false
	}
	if !topology.Complete() || rr.Spec.Size == nil || topology.Observed() != int(*rr.Spec.Size) {
		return false
	}
	groups, err := listSentinelGroups(ctx, r.Client, rr)
	if err != nil || len(groups) == 0 {
		return false
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(rr.Namespace), client.MatchingLabels(
		common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil),
	)); err != nil {
		return false
	}
	confirmedMaster := ""
	if len(topology.Masters) == 1 && !r.isLoneMasterEmptyReplacement(ctx, rr, topology) {
		confirmedMaster = topology.Masters[0]
	}
	checker := redishealer.NewChecker(r.K8sClient)
	redisClient := r.SentinelClient
	if redisClient == nil {
		redisClient = redis.NewClient()
	}
	anyDeadOrStuckGroup := false
	for _, group := range groups {
		if len(group.pods) == 0 {
			return false
		}
		password, err := checker.GetPassword(ctx, rr.Namespace, group.password)
		if err != nil {
			return false
		}
		var (
			mu         sync.Mutex
			wg         sync.WaitGroup
			answers    []string
			identities = map[string]bool{}
			duplicate  bool
		)
		for _, pod := range group.pods {
			if pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
				continue
			}
			connInfo := redishealer.ConnectionInfoForPod(ctx, pod, password, group.tls, r.K8sClient, rr.Namespace, "26379")
			wg.Add(1)
			go func() {
				defer wg.Done()
				queryCtx, cancel := context.WithTimeout(ctx, sentinelQueryTimeout)
				defer cancel()
				service := redisClient.Connect(connInfo)
				identity, err := service.SentinelMyID(queryCtx)
				if err != nil || identity == "" {
					return
				}
				mu.Lock()
				if identities[identity] {
					duplicate = true
				}
				identities[identity] = true
				mu.Unlock()
				host, port, monitored, err := service.SentinelGetMasterAddrByName(queryCtx, group.masterGroup)
				if err != nil {
					return
				}
				answer := ""
				if monitored {
					answer = canonicalSentinelAddress(pods.Items, net.JoinHostPort(host, port))
				}
				mu.Lock()
				answers = append(answers, answer)
				mu.Unlock()
			}()
		}
		wg.Wait()
		if duplicate {
			return false
		}
		liveElectorate := max(group.size, int32(len(group.pods)))
		if liveElectorate <= 0 || int32(len(answers)) <= liveElectorate/2 {
			return false
		}
		hasDeadOrPlaceholder := false
		for _, addr := range answers {
			if isUnspecifiedSentinelAddr(addr) {
				hasDeadOrPlaceholder = true
				continue
			}
			podName := podForAddress(pods.Items, addr)
			if podName == "" {
				hasDeadOrPlaceholder = true
				continue
			}
			if confirmedMaster != "" && podName == confirmedMaster {
				continue
			}
			return false
		}
		if hasDeadOrPlaceholder {
			anyDeadOrStuckGroup = true
		}
	}
	return anyDeadOrStuckGroup
}
