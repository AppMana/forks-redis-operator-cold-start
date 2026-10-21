package k8sutils

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/go-redis/redismock/v9"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const servingClusterInfo = "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n"

func TestClusterTopologySharesDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	calls := 0
	err := checkClusterTopology(ctx, 3, func(ctx context.Context, _ int32) error {
		calls++
		<-ctx.Done()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, calls, "an expired budget must not start more seed checks")
}

func TestClusterTopologyFallbackWithoutInlineRetries(t *testing.T) {
	for _, successAt := range []int32{-1, 0, 2} {
		calls := int32(0)
		err := checkClusterTopology(context.Background(), 3, func(ctx context.Context, i int32) error {
			calls++
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), clusterCheckExecTimeout)
			if i == successAt {
				return nil
			}
			return errors.New("seed unavailable")
		})
		if successAt < 0 {
			require.Error(t, err)
			require.Equal(t, int32(3), calls)
		} else {
			require.NoError(t, err)
			require.Equal(t, successAt+1, calls)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, checkClusterTopology(ctx, 3, func(context.Context, int32) error {
		t.Fatal("cancelled check must not execute")
		return nil
	}), context.Canceled)
}

func TestValidateClusterServingInfo(t *testing.T) {
	require.NoError(t, validateClusterServingInfo(servingClusterInfo+"cluster_known_nodes:6\r\n"))
	for _, info := range []string{
		"", "cluster_state:ok\r\n",
		strings.ReplaceAll(servingClusterInfo, "cluster_state:ok", "cluster_state:fail"),
		strings.ReplaceAll(servingClusterInfo, "cluster_slots_ok:16384", "cluster_slots_ok:16383"),
		strings.ReplaceAll(servingClusterInfo, "cluster_slots_pfail:0", "cluster_slots_pfail:1"),
		strings.ReplaceAll(servingClusterInfo, "cluster_slots_fail:0", "cluster_slots_fail:1"),
		servingClusterInfo + "cluster_state:ok\r\n",
	} {
		require.Error(t, validateClusterServingInfo(info))
	}
}

func TestClusterNodesServingChecksEveryNode(t *testing.T) {
	cr := &rcvb2.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "test"}, Spec: rcvb2.RedisClusterSpec{ClusterSize: ptr.To(int32(3))}}
	for _, failing := range []string{"", "test-leader-2", "test-follower-2", "read-error"} {
		t.Run("failure-"+failing, func(t *testing.T) {
			var visited []string
			err := clusterNodesServing(context.Background(), cr, func(pod string) *redis.Client {
				visited = append(visited, pod)
				client, mock := redismock.NewClientMock()
				t.Cleanup(func() { require.NoError(t, mock.ExpectationsWereMet()) })
				if failing == "read-error" {
					mock.ExpectClusterInfo().SetErr(errors.New("unreachable"))
				} else if pod == failing {
					mock.ExpectClusterInfo().SetVal(strings.ReplaceAll(servingClusterInfo, "cluster_state:ok", "cluster_state:fail"))
				} else {
					mock.ExpectClusterInfo().SetVal(servingClusterInfo)
				}
				return client
			})
			if failing == "" {
				require.NoError(t, err)
				require.Equal(t, []string{"test-leader-0", "test-leader-1", "test-leader-2", "test-follower-0", "test-follower-1", "test-follower-2"}, visited)
			} else {
				require.Error(t, err)
			}
		})
	}
}
