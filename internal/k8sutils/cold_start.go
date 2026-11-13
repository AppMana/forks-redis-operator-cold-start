/*
Cold-start bootstrap recovery for RedisReplication.

The upstream reconciler's master detection relies on INFO replication output.
On a simultaneous restart of all pods, every fresh Redis reports role:master
and GetRedisReplicationRealMaster returns "" because none have attached slaves.
Similarly, if every pod came back as role:slave pointing at a ghost (dead)
master IP, masterNodes is empty and the reconciler has no branch that handles
it. Both states leave the cluster wedged.

PickPreferredMaster and PromoteMasterAndReslave give the reconciler a
deterministic way out of these states, preferring the CR's persisted
.status.masterNode when present, otherwise the lowest-ordinal candidate.
*/

package k8sutils

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ReplicationNodeInfo captures the key fields of INFO replication for a pod.
type ReplicationNodeInfo struct {
	Role             string
	ReplID           string
	MasterReplOffset int64
	SlaveReplOffset  int64
	ConnectedSlaves  int
	MasterHost       string
	MasterPort       string
	MasterLinkStatus string
}

// EffectiveOffset returns the node's replication progress offset, preferring
// the larger of master_repl_offset and slave_repl_offset.
func (info ReplicationNodeInfo) EffectiveOffset() int64 {
	if info.SlaveReplOffset > info.MasterReplOffset {
		return info.SlaveReplOffset
	}
	return info.MasterReplOffset
}

// ParseReplicationNodeInfo extracts role, replication ID, offsets, connected
// slave count, and upstream master link fields from INFO replication output.
func ParseReplicationNodeInfo(raw string) (ReplicationNodeInfo, error) {
	var info ReplicationNodeInfo
	for _, line := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch key {
		case "role":
			info.Role = value
		case "master_replid":
			info.ReplID = value
		case "master_repl_offset":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				info.MasterReplOffset = v
			}
		case "slave_repl_offset":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				info.SlaveReplOffset = v
			}
		case "connected_slaves":
			if v, err := strconv.Atoi(value); err == nil {
				info.ConnectedSlaves = v
			}
		case "master_host":
			info.MasterHost = value
		case "master_port":
			info.MasterPort = value
		case "master_link_status":
			info.MasterLinkStatus = value
		}
	}
	return info, nil
}

// ObserveReplicationNodeInfos queries INFO replication on each probeable pod in pods.
func ObserveReplicationNodeInfos(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, pods []string) map[string]ReplicationNodeInfo {
	result := make(map[string]ReplicationNodeInfo, len(pods))
	if client == nil || cr == nil {
		return result
	}
	for _, podName := range pods {
		pod, err := client.CoreV1().Pods(cr.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil || !IsRedisPodProbeable(pod) {
			continue
		}
		c := configureRedisReplicationClientForPod(ctx, client, cr, pod)
		probeCtx, cancel := context.WithTimeout(ctx, RoleProbeTimeout)
		raw, err := c.Info(probeCtx, "replication").Result()
		cancel()
		c.Close()
		if err != nil {
			continue
		}
		if parsed, err := ParseReplicationNodeInfo(raw); err == nil && parsed.Role != "" {
			result[podName] = parsed
		}
	}
	return result
}

// ValidateRecoveryLineage refuses to discard a nonempty peer unless the
// candidate contains at least its replication offset in the same history.
// Offsets from different replication IDs are not comparable. Empty peers
// are safe to reattach, but missing observations are never treated as empty.
func ValidateRecoveryLineage(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, pods []string, candidate string, counts map[string]int64) error {
	return observeRecoveryLineage(pods, candidate, counts, func(pod string) (string, error) {
		c := configureRedisReplicationClient(ctx, client, cr, pod)
		defer c.Close()
		return c.Info(ctx, "replication").Result()
	})
}

func observeRecoveryLineage(pods []string, candidate string, counts map[string]int64, observe func(string) (string, error)) error {
	infos := map[string]string{}
	// Sample the candidate last: a live primary's stream can advance while
	// peers are being queried. Sampling it first can make caught-up replicas
	// appear ahead forever under continuous traffic. The history/offset checks
	// below still reject genuinely newer or divergent peer data.
	ordered := make([]string, 0, len(pods))
	found := false
	for _, pod := range pods {
		if pod == candidate {
			found = true
		} else {
			ordered = append(ordered, pod)
		}
	}
	if !found {
		return errors.New("recovery candidate is not in the observed topology")
	}
	ordered = append(ordered, candidate)
	for _, pod := range ordered {
		count, ok := counts[pod]
		if !ok {
			return fmt.Errorf("recovery key count unavailable for %s", pod)
		}
		if count == 0 {
			continue
		}
		info, err := observe(pod)
		if err != nil {
			return fmt.Errorf("recovery lineage unavailable for %s: %w", pod, err)
		}
		infos[pod] = info
	}
	return validateRecoveryLineage(pods, candidate, counts, infos)
}

func validateRecoveryLineage(pods []string, candidate string, counts map[string]int64, infos map[string]string) error {
	lineage := func(pod string) (string, int64, error) {
		fields := map[string]string{}
		for _, line := range strings.Split(infos[pod], "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
			if ok {
				fields[key] = value
			}
		}
		offset, err := strconv.ParseInt(fields["master_repl_offset"], 10, 64)
		if slaveOffset, sErr := strconv.ParseInt(fields["slave_repl_offset"], 10, 64); sErr == nil && (err != nil || slaveOffset > offset) {
			offset = slaveOffset
			err = nil
		}
		id := fields["master_replid"]
		if err != nil || offset < 0 || id == "" || strings.Trim(id, "0") == "" {
			return "", 0, fmt.Errorf("invalid replication history for %s", pod)
		}
		return id, offset, nil
	}
	candidateKeys, ok := counts[candidate]
	if candidate == "" || !ok {
		return fmt.Errorf("recovery candidate is unobserved")
	}
	for _, pod := range pods {
		count, ok := counts[pod]
		if !ok {
			return fmt.Errorf("recovery key count unavailable for %s", pod)
		}
		if pod == candidate || count == 0 {
			continue
		}
		if candidateKeys == 0 {
			return fmt.Errorf("refusing empty candidate %s while %s holds data", candidate, pod)
		}
		id, offset, err := lineage(candidate)
		if err != nil {
			return err
		}
		peerID, peerOffset, err := lineage(pod)
		if err != nil {
			return err
		}
		if id != peerID || offset < peerOffset {
			return fmt.Errorf("ambiguous recovery history between %s and %s; refusing automatic demotion", candidate, pod)
		}
	}
	return nil
}

// PickPreferredMaster returns the preferred master pod name from a list of
// candidates. Preference: (1) CR status.masterNode if it's in candidates;
// (2) lowest-ordinal candidate (callers pass ordinal-sorted slices). Returns
// "" only when candidates is empty.
func PickPreferredMaster(cr *rrvb2.RedisReplication, candidates []string) string {
	if cr != nil && cr.Status.MasterNode != "" {
		for _, c := range candidates {
			if c == cr.Status.MasterNode {
				return c
			}
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

// SelectRecoveryMaster chooses the best primary candidate across allPods during
// cold-start or dead-master recovery.
//
// Rules:
//  1. If any pod holds data (counts[pod] > 0) or replication progress
//     (EffectiveOffset() > 0), empty pods (counts[pod] == 0 and EffectiveOffset() == 0)
//     are excluded so an empty restarted pod is never promoted over data-holding survivors.
//  2. Among eligible pods, if replication offsets are known and > 0, only pods
//     at the highest observed EffectiveOffset() are kept.
//  3. Ties are broken by preferring pods already in masterNodes (if any are in
//     the tied highest-offset set), then by PickPreferredMaster (Status.MasterNode,
//     then lowest ordinal).
func SelectRecoveryMaster(
	cr *rrvb2.RedisReplication,
	masterNodes []string,
	slaveNodes []string,
	counts map[string]int64,
	infos map[string]ReplicationNodeInfo,
) string {
	allPods := append(append([]string{}, masterNodes...), slaveNodes...)
	if len(allPods) == 0 {
		return ""
	}

	hasNonEmpty := false
	for _, pod := range allPods {
		if counts[pod] > 0 || infos[pod].EffectiveOffset() > 0 {
			hasNonEmpty = true
			break
		}
	}

	var eligible []string
	for _, pod := range allPods {
		if hasNonEmpty && counts[pod] <= 0 && infos[pod].EffectiveOffset() <= 0 {
			continue
		}
		eligible = append(eligible, pod)
	}
	if len(eligible) == 0 {
		eligible = allPods
	}

	var maxOffset int64 = -1
	for _, pod := range eligible {
		if info, ok := infos[pod]; ok {
			if off := info.EffectiveOffset(); off > maxOffset {
				maxOffset = off
			}
		}
	}

	if maxOffset > 0 {
		var highest []string
		for _, pod := range eligible {
			if info, ok := infos[pod]; ok && info.EffectiveOffset() == maxOffset {
				highest = append(highest, pod)
			}
		}
		if len(highest) > 0 {
			eligible = highest
		}
	}

	var masterEligible []string
	for _, pod := range eligible {
		if slices.Contains(masterNodes, pod) {
			masterEligible = append(masterEligible, pod)
		}
	}
	if len(masterEligible) > 0 {
		return PickPreferredMaster(cr, masterEligible)
	}
	return PickPreferredMaster(cr, eligible)
}

// PromoteMasterAndReslave forces targetMaster to become master via REPLICAOF
// NO ONE, then reslaves every other pod in allPods to it. Used when no pod
// in the replication is currently a valid master (e.g. every pod is a slave
// pointed at a dead IP after a cold restart).
func PromoteMasterAndReslave(ctx context.Context, client kubernetes.Interface, cr *rrvb2.RedisReplication, allPods []string, targetMaster string) error {
	if targetMaster == "" {
		return errors.New("PromoteMasterAndReslave: empty target master")
	}
	targetIP := getRedisServerIP(ctx, client, RedisDetails{PodName: targetMaster, Namespace: cr.Namespace})
	if targetIP == "" {
		return fmt.Errorf("PromoteMasterAndReslave: could not resolve IP for %s", targetMaster)
	}

	promoteClient := configureRedisReplicationClient(ctx, client, cr, targetMaster)
	defer promoteClient.Close()
	if err := promoteReplicationClient(ctx, promoteClient); err != nil {
		return fmt.Errorf("promote %s to master: %w", targetMaster, err)
	}
	log.FromContext(ctx).Info("Promoted pod to master", "pod", targetMaster)

	for _, pod := range allPods {
		if pod == targetMaster {
			continue
		}
		c := configureRedisReplicationClient(ctx, client, cr, pod)
		if err := reconfigureReplicationClient(ctx, c, targetIP); err != nil {
			c.Close()
			return fmt.Errorf("slave %s of %s: %w", pod, targetMaster, err)
		}
		c.Close()
		log.FromContext(ctx).Info("Reslaved pod", "pod", pod, "to", targetMaster)
	}
	return nil
}

// RecordedMasterAmongUnrelatedMasters returns the recorded master when every
// configured pod is an unattached master and the recorded master holds data.
//
// That is the state after every pod of a persistent replication restarted
// from its own disk: each one loads its copy of the dataset, reports
// role:master and generates a new replication ID. No pod replicates from
// another, so the histories share nothing that ValidateRecoveryLineage could
// compare, and the offsets only count what each pod did since it started.
// Waiting cannot resolve it either: the IDs never converge on their own.
//
// The record is then the only remaining statement about which copy was the
// primary: it names the last pod that was positively identified as the elected
// master and published for writes, and the other pods were its replicas. It
// is used only when it is not contradicted: the pod is observed as a master,
// it holds keys, and no master has an attached replica. A master that shares
// the recorded master's replication ID has a comparable history, which stays
// subject to the lineage rules instead. Missing observations never count as
// empty or unattached.
//
// Callers must separately establish that no Sentinel holds an election for
// another live pod.
func RecordedMasterAmongUnrelatedMasters(cr *rrvb2.RedisReplication, masters, slaves []string, counts map[string]int64, infos map[string]ReplicationNodeInfo) (string, bool) {
	if cr == nil || cr.Spec.Size == nil || len(slaves) != 0 || len(masters) < 2 || len(masters) != int(*cr.Spec.Size) {
		return "", false
	}
	recorded := cr.Status.MasterNode
	if recorded == "" || !slices.Contains(masters, recorded) {
		return "", false
	}
	recordedInfo, ok := infos[recorded]
	if !ok || recordedInfo.ReplID == "" || strings.Trim(recordedInfo.ReplID, "0") == "" {
		return "", false
	}
	if keys, ok := counts[recorded]; !ok || keys <= 0 {
		return "", false
	}
	for _, master := range masters {
		info, observed := infos[master]
		if _, counted := counts[master]; !counted || !observed || info.Role != "master" || info.ConnectedSlaves != 0 {
			return "", false
		}
		if master != recorded && info.ReplID == recordedInfo.ReplID {
			return "", false
		}
	}
	return recorded, true
}
