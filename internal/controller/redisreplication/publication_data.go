package redisreplication

import (
	"context"
	"fmt"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// masterDataSafeForPublication applies the empty-primary safeguard before a
// client can write through the master Service. Topology repair alone is too
// late: a replacement at the same FQDN can still be Sentinel's elected primary.
// A nonempty candidate needs just one bounded probe, not a probe of dead peers.
// Unknown candidate data prevents new publication but is not proof sufficient
// to withdraw an existing healthy primary on a transient timeout.
func (r *Reconciler) masterDataSafeForPublication(ctx context.Context, instance *rrvb2.RedisReplication, primary string) (publishable, rejectEmpty bool) {
	if r.Checker == nil {
		return false, false
	}
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	counts := r.Checker.GetReplicationKeyCounts(probeCtx, instance, []string{primary})
	cancel()
	keys, known := counts[primary]
	if !known || keys < 0 {
		return false, false
	}
	if keys > 0 {
		return true, false
	}
	if instance.Spec.Size == nil || *instance.Spec.Size < 1 {
		return false, true
	}
	peers := make([]string, 0, *instance.Spec.Size)
	for ordinal := int32(0); ordinal < *instance.Spec.Size; ordinal++ {
		pod := fmt.Sprintf("%s-%d", instance.GetStatefulSetName(), ordinal)
		if pod != primary {
			peers = append(peers, pod)
		}
	}
	probeCtx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	counts = r.Checker.GetReplicationKeyCounts(probeCtx, instance, peers)
	for _, pod := range peers {
		if count, known := counts[pod]; !known || count != 0 {
			log.FromContext(ctx).Info("Withholding empty primary from writable Service; peer data is nonempty or unknown", "primary", primary, "peer", pod)
			return false, true
		}
	}
	return true, false
}
