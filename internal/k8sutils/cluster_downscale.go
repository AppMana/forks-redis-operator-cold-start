package k8sutils

import (
	"context"
	"fmt"
	"strings"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	redis "github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
)

// VerifyClusterScaleDownReady proves that every retained pod sees only retained
// members and a serving cluster before StatefulSet reconciliation removes pods.
// A failed count lookup is not evidence that resharding/removal has completed.
func VerifyClusterScaleDownReady(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	return verifyClusterScaleDownReady(ctx, cr, func(pod string) *redis.Client {
		return configureRedisClient(ctx, client, cr, pod)
	})
}

func verifyClusterScaleDownReady(ctx context.Context, cr *rcvb2.RedisCluster, makeClient func(string) *redis.Client) error {
	leaders := cr.Spec.GetReplicaCounts("leader")
	if leaders < 1 {
		return fmt.Errorf("cannot downscale to zero leaders")
	}
	retained := map[string]bool{}
	var clients []*redis.Client
	for _, role := range []string{"leader", "follower"} {
		for i := int32(0); i < cr.Spec.GetReplicaCounts(role); i++ {
			c := makeClient(fmt.Sprintf("%s-%s-%d", cr.Name, role, i))
			defer c.Close()
			id, err := c.Do(ctx, "CLUSTER", "MYID").Text()
			if err != nil || id == "" || retained[id] {
				return fmt.Errorf("retained node identity inventory is incomplete or ambiguous")
			}
			retained[id] = true
			clients = append(clients, c)
		}
	}
	for _, c := range clients {
		info, err := c.ClusterInfo(ctx).Result()
		if err != nil || validateClusterServingInfo(info) != nil {
			return fmt.Errorf("retained node does not report a serving cluster")
		}
		nodes, err := clusterNodes(ctx, c)
		if err != nil {
			return fmt.Errorf("retained cluster membership is unavailable")
		}
		if err := validateRetainedMembership(nodes, retained, leaders); err != nil {
			return err
		}
	}
	return nil
}

func validateRetainedMembership(nodes []clusterNodesResponse, retained map[string]bool, leaders int32) error {
	if len(nodes) != len(retained) || len(retained) == 0 {
		return fmt.Errorf("cluster membership does not match retained pods")
	}
	seen := map[string]bool{}
	masters := int32(0)
	for _, node := range nodes {
		if len(node) < 8 || !retained[node[0]] || seen[node[0]] {
			return fmt.Errorf("cluster membership contains unknown, duplicate or malformed nodes")
		}
		seen[node[0]] = true
		if strings.Contains(node[2], "fail") || hasAnyFlag(node[2], "handshake", "noaddr") || node[7] != "connected" {
			return fmt.Errorf("retained cluster membership is unhealthy")
		}
		if hasFlag(node[2], "master") {
			masters++
		} else if !hasFlag(node[2], "slave") {
			return fmt.Errorf("retained node role is unknown")
		}
	}
	if masters != leaders || clusterHasOpenSlots(nodes) {
		return fmt.Errorf("retained cluster topology has not converged")
	}
	return nil
}
