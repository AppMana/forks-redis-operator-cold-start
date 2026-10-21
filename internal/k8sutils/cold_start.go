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
	"strconv"
	"strings"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
	if err := promoteClient.SlaveOf(ctx, "NO", "ONE").Err(); err != nil {
		return fmt.Errorf("promote %s to master: %w", targetMaster, err)
	}
	log.FromContext(ctx).Info("Promoted pod to master", "pod", targetMaster)

	for _, pod := range allPods {
		if pod == targetMaster {
			continue
		}
		c := configureRedisReplicationClient(ctx, client, cr, pod)
		if err := c.SlaveOf(ctx, targetIP, "6379").Err(); err != nil {
			c.Close()
			return fmt.Errorf("slave %s of %s: %w", pod, targetMaster, err)
		}
		c.Close()
		log.FromContext(ctx).Info("Reslaved pod", "pod", pod, "to", targetMaster)
	}
	return nil
}
