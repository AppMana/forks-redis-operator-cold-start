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

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
