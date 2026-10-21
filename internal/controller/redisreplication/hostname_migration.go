package redisreplication

import (
	"context"
	"fmt"
	"slices"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Finish the Sentinel rollout and move its live monitors to stable addresses
// before changing Redis templates. Otherwise both sets can restart together,
// leaving retained monitors pointing at IPs that no longer belong to any pod.
func (r *Reconciler) prepareSentinelHostnameMigration(ctx context.Context, rr *rrvb2.RedisReplication, sentinel appsv1.StatefulSet) error {
	if rr.Spec.Sentinel.ResolveHostnames != "yes" {
		return nil
	}
	var current appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: rr.Namespace, Name: rr.Name}, &current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		} // First installation has no Redis pods to protect.
		return err
	}
	resolve, announce := "no", "no"
	for _, c := range current.Spec.Template.Spec.Containers {
		if c.Name != rr.Name {
			continue
		}
		for _, env := range c.Env {
			if env.Name == "RESOLVE_HOSTNAMES" {
				resolve = env.Value
			}
			if env.Name == "ANNOUNCE_HOSTNAMES" {
				announce = env.Value
			}
		}
	}
	if resolve == "yes" && announce == resolveHostnamesOrDefault(rr.Spec.Sentinel.AnnounceHostnames) {
		return nil
	}
	if sentinel.Status.ObservedGeneration != sentinel.Generation || sentinel.Status.ReadyReplicas != rr.Spec.Sentinel.Size || sentinel.Status.UpdatedReplicas != rr.Spec.Sentinel.Size || sentinel.Status.CurrentRevision != sentinel.Status.UpdateRevision {
		return fmt.Errorf("waiting for Sentinel rollout before enabling Redis hostname announcements")
	}
	topology, err := r.redisReplicationTopology(ctx, rr)
	if err != nil {
		return err
	}
	if !topology.Complete() {
		return fmt.Errorf("cannot migrate hostnames with incomplete Redis topology")
	}
	master, authoritative := r.sentinelElectedMaster(ctx, rr)
	if authoritative {
		if master == "" || !slices.Contains(topology.Masters, master) {
			if master == "" {
				// Resource preparation runs before reconcileRedis. An ephemeral
				// rollout may have initialized only one monitor, so waiting here
				// without resuming guarded bootstrap would prevent quorum forever.
				if _, err := r.initializeEmbeddedSentinelPlaceholder(ctx, rr, topology); err != nil {
					return fmt.Errorf("resume Sentinel bootstrap during hostname migration: %w", err)
				}
			}
			return fmt.Errorf("cannot migrate hostnames with unresolved Sentinel authority")
		}
	} else {
		// An ephemeral Sentinel rollout can restart with no monitor. Only an
		// unambiguous, already-linked Redis primary may seed those monitors.
		if len(topology.Masters) != 1 {
			return fmt.Errorf("cannot seed Sentinel monitors from ambiguous Redis masters")
		}
		master = topology.Masters[0]
		if rr.Spec.Size == nil || (*rr.Spec.Size > 1 && r.redisReplicationRealMaster(ctx, rr, topology.Masters) != master) {
			return fmt.Errorf("cannot seed Sentinel monitors without an established Redis primary")
		}
	}
	return r.configureReplicationSentinel(ctx, rr, master)
}
