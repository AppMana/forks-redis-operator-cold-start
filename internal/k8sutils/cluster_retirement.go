package k8sutils

import (
	"context"
	"fmt"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	redis "github.com/redis/go-redis/v9"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Snapshot attached replica identities before resharding can reparent them.
// Attachment alone is not retirement intent: retain every desired pod ordinal.
func retiringFollowerIDs(ctx context.Context, kube kubernetes.Interface, cr *rcvb2.RedisCluster, seed *redis.Client, source string) ([]string, error) {
	sts, err := kube.AppsV1().StatefulSets(cr.Namespace).Get(ctx, cr.Name+"-follower", metav1.GetOptions{})
	desired := cr.Spec.GetReplicaCounts("follower")
	if apierrors.IsNotFound(err) && desired == 0 {
		return nil, nil
	}
	if err != nil || sts.Spec.Replicas == nil {
		return nil, fmt.Errorf("follower replica inventory is unavailable")
	}
	obsolete := map[string]bool{}
	seen := map[string]bool{}
	for i := int32(0); i < *sts.Spec.Replicas; i++ {
		id := getRedisNodeID(ctx, kube, cr, RedisDetails{PodName: fmt.Sprintf("%s-follower-%d", cr.Name, i), Namespace: cr.Namespace})
		if id == "" || id == source || seen[id] {
			return nil, fmt.Errorf("follower identity is unavailable or ambiguous")
		}
		seen[id] = true
		if i >= desired {
			obsolete[id] = true
		}
	}
	if len(obsolete) == 0 {
		return nil, nil
	}
	attached, err := getAttachedFollowerNodeIDs(ctx, seed, source)
	if err != nil {
		return nil, err
	}
	return selectRetiringFollowers(attached, obsolete), nil
}

func selectRetiringFollowers(attached []string, obsolete map[string]bool) []string {
	var ids []string
	for _, id := range attached {
		if obsolete[id] {
			ids = append(ids, id)
		}
	}
	return ids
}

func retireEmptyFollowers(ids []string, observe func(string) (int, error), remove func(string) error) error {
	for _, id := range ids {
		slots, err := observe(id)
		if err != nil || slots != 0 {
			return fmt.Errorf("follower retirement blocked by incomplete or nonempty slot ownership")
		}
	}
	for _, id := range ids {
		if err := remove(id); err != nil {
			return err
		}
	}
	return nil
}
