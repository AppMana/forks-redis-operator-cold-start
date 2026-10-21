package k8sutils

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestClusterServingIntegration(t *testing.T) {
	if os.Getenv("REDIS_SERVING_TEST") != "1" {
		t.Skip("isolated Redis container required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	server := exec.CommandContext(ctx, "redis-server", "--cluster-enabled", "yes", "--cluster-config-file", "/tmp/serving-nodes.conf", "--cluster-announce-ip", "127.0.0.1", "--save", "", "--appendonly", "no")
	require.NoError(t, server.Start())
	t.Cleanup(func() { _ = server.Process.Kill(); _ = server.Wait() })
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	require.Eventually(t, func() bool { return client.Ping(ctx).Err() == nil }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, client.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", 0, 16383).Err())
	info, err := client.ClusterInfo(ctx).Result()
	require.NoError(t, err)
	require.Contains(t, info, "cluster_state:fail")
	out, err := exec.CommandContext(ctx, "redis-cli", "--cluster", "check", "127.0.0.1:6379").Output()
	require.NoError(t, err)
	require.Equal(t, 3, strings.Count(string(out), "[OK]"), "old topology-only check would accept this cluster")
	require.ErrorContains(t, client.Get(ctx, "readiness-probe").Err(), "CLUSTERDOWN")
	require.Error(t, validateClusterServingInfo(info), "new serving check must reject the false positive")
	require.Eventually(t, func() bool {
		info, err := client.ClusterInfo(ctx).Result()
		return err == nil && validateClusterServingInfo(info) == nil
	}, 8*time.Second, 25*time.Millisecond)
	require.ErrorIs(t, client.Get(ctx, "readiness-probe").Err(), redis.Nil)
	t.Run("failover error is not acknowledgement", func(t *testing.T) {
		out, err := exec.CommandContext(ctx, "redis-cli", "--raw", "CLUSTER", "FAILOVER").Output()
		require.NoError(t, err, "redis-cli exits zero despite Redis rejecting failover on a master")
		require.Contains(t, string(out), "ERR")
		require.EqualError(t, validateFailoverAcknowledgement(string(out), err), "redis cluster failover was not acknowledged")
	})
}
