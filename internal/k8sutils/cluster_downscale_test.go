package k8sutils

import (
	"context"
	"errors"
	"testing"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/go-redis/redismock/v9"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestRetainedMembershipBlocksUnsafeShrink(t *testing.T) {
	valid := clusterNodesResponse{"a", "127.0.0.1:6379", "master", "-", "0", "0", "1", "connected", "0-16383"}
	retained := map[string]bool{"a": true}
	require.NoError(t, validateRetainedMembership([]clusterNodesResponse{valid}, retained, 1))
	for _, tc := range []struct {
		name  string
		nodes []clusterNodesResponse
	}{
		{"empty", nil},
		{"unknown retiring node", []clusterNodesResponse{valid, {"old", "addr", "master", "-", "0", "0", "1", "connected"}}},
		{"malformed", []clusterNodesResponse{{"a"}}},
		{"wrong identity", []clusterNodesResponse{{"old", "addr", "master", "-", "0", "0", "1", "connected"}}},
		{"probably failed", []clusterNodesResponse{{"a", "addr", "master,fail?", "-", "0", "0", "1", "connected"}}},
		{"disconnected", []clusterNodesResponse{{"a", "addr", "master", "-", "0", "0", "1", "disconnected"}}},
		{"open migration", []clusterNodesResponse{{"a", "addr", "master", "-", "0", "0", "1", "connected", "0-16383", "[1->-old]"}}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Error(t, validateRetainedMembership(tc.nodes, retained, 1)) })
	}
}

func TestScaleDownChecksEveryRetainedView(t *testing.T) {
	cr := &rcvb2.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "test"}, Spec: rcvb2.RedisClusterSpec{ClusterSize: ptr.To(int32(1))}}
	nodes := "a 127.0.0.1:6379 master - 0 0 1 connected 0-16383\nb 127.0.0.2:6379 slave a 0 0 1 connected\n"
	for _, badFollower := range []bool{false, true} {
		t.Run(map[bool]string{false: "converged", true: "follower lookup fails"}[badFollower], func(t *testing.T) {
			err := verifyClusterScaleDownReady(context.Background(), cr, func(pod string) *redis.Client {
				c, m := redismock.NewClientMock()
				t.Cleanup(func() { require.NoError(t, m.ExpectationsWereMet()) })
				id := "a"
				if pod == "test-follower-0" {
					id = "b"
				}
				m.ExpectDo("CLUSTER", "MYID").SetVal(id)
				m.ExpectClusterInfo().SetVal(servingClusterInfo)
				if id == "b" && badFollower {
					m.ExpectClusterNodes().SetErr(errors.New("unreachable"))
				} else {
					m.ExpectClusterNodes().SetVal(nodes)
				}
				return c
			})
			if badFollower {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
