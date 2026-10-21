package k8sutils

import (
	"context"
	"fmt"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	redis "github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
)

// RetireObsoleteClusterFollowers handles ordinal retirement independently of
// shard attachment, including follower-only downscales and reparented replicas.
// StatefulSets remain unchanged until VerifyClusterScaleDownReady succeeds.
func RetireObsoleteClusterFollowers(ctx context.Context, kube kubernetes.Interface, cr *rcvb2.RedisCluster, current int32) error {
	desired := cr.Spec.GetReplicaCounts("follower")
	if desired >= current {
		return nil
	}
	protected := map[string]bool{}
	for _, role := range []string{"leader", "follower"} {
		for i := int32(0); i < cr.Spec.GetReplicaCounts(role); i++ {
			c := configureRedisClient(ctx, kube, cr, fmt.Sprintf("%s-%s-%d", cr.Name, role, i))
			defer c.Close()
			id, err := c.Do(ctx, "CLUSTER", "MYID").Text()
			if err != nil || id == "" || protected[id] {
				return fmt.Errorf("retained identity inventory is unavailable or ambiguous")
			}
			protected[id] = true
			info, err := c.ClusterInfo(ctx).Result()
			if err != nil || validateClusterServingInfo(info) != nil {
				return fmt.Errorf("cannot retire follower copies before retained nodes are serving")
			}
		}
	}
	seed := configureRedisClient(ctx, kube, cr, cr.Name+"-leader-0")
	defer seed.Close()
	return retireFollowerOrdinals(current, desired, protected, func(i int32) string {
		return getRedisNodeID(ctx, kube, cr, RedisDetails{PodName: fmt.Sprintf("%s-follower-%d", cr.Name, i), Namespace: cr.Namespace})
	}, func(id string) (bool, error) {
		return clusterLeaderRetired(ctx, cr, id, func(pod string) *redis.Client {
			return configureRedisClient(ctx, kube, cr, pod)
		})
	}, func(id string) (int, error) {
		return ownedRedisClusterSlots(ctx, seed, id)
	}, func(id string) error { return RemoveRedisNodeFromCluster(ctx, kube, cr, id) })
}

func retireFollowerOrdinals(current, desired int32, protected map[string]bool, identity func(int32) string, retired func(string) (bool, error), observe func(string) (int, error), remove func(string) error) error {
	if desired < 0 || current < desired || len(protected) == 0 {
		return fmt.Errorf("invalid follower retirement inventory")
	}
	seen := map[string]bool{}
	var candidates []string
	for i := current - 1; i >= desired; i-- {
		id := identity(i)
		if id == "" || protected[id] || seen[id] {
			return fmt.Errorf("obsolete follower identity is unavailable or ambiguous")
		}
		seen[id] = true
		done, err := retired(id)
		if err != nil {
			return err
		}
		if !done {
			candidates = append(candidates, id)
		}
	}
	return retireEmptyFollowers(candidates, observe, remove)
}
