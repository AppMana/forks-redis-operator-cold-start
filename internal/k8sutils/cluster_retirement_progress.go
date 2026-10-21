package k8sutils

import (
	"context"
	"fmt"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	redis "github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
)

// ClusterLeaderRetired identifies completed retirements from live identities,
// not a master count that changes halfway through a multi-shard downscale.
func ClusterLeaderRetired(ctx context.Context, kube kubernetes.Interface, cr *rcvb2.RedisCluster, ordinal int32) (bool, error) {
	if ordinal < cr.Spec.GetReplicaCounts("leader") {
		return false, fmt.Errorf("cannot retire a retained leader")
	}
	source := getRedisNodeID(ctx, kube, cr, RedisDetails{
		PodName: fmt.Sprintf("%s-leader-%d", cr.Name, ordinal), Namespace: cr.Namespace,
	})
	return clusterLeaderRetired(ctx, cr, source, func(pod string) *redis.Client {
		return configureRedisClient(ctx, kube, cr, pod)
	})
}

func clusterLeaderRetired(ctx context.Context, cr *rcvb2.RedisCluster, source string, makeClient func(string) *redis.Client) (bool, error) {
	if source == "" || cr.Spec.GetReplicaCounts("leader") < 1 {
		return false, fmt.Errorf("retirement identity inventory is unavailable")
	}
	var present, absent int
	for _, role := range []string{"leader", "follower"} {
		for i := int32(0); i < cr.Spec.GetReplicaCounts(role); i++ {
			c := makeClient(fmt.Sprintf("%s-%s-%d", cr.Name, role, i))
			defer c.Close()
			nodes, err := clusterNodes(ctx, c)
			if err != nil || len(nodes) == 0 {
				return false, fmt.Errorf("retirement membership observation is unavailable")
			}
			seen := map[string]bool{}
			for _, node := range nodes {
				if len(node) < 8 || node[0] == "" || seen[node[0]] {
					return false, fmt.Errorf("retirement membership observation is malformed")
				}
				seen[node[0]] = true
			}
			if seen[source] {
				present++
				continue
			}
			info, err := c.ClusterInfo(ctx).Result()
			if err != nil || validateClusterServingInfo(info) != nil {
				return false, fmt.Errorf("cannot confirm retirement from a non-serving retained node")
			}
			absent++
		}
	}
	if present > 0 && absent > 0 {
		return false, fmt.Errorf("retained nodes disagree about shard retirement")
	}
	return absent > 0, nil
}
