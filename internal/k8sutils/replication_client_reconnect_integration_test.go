package k8sutils

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Run the compiled test inside a dedicated --network none Redis 7.0.15
// container. Pin its run_id through a prior read; never use a shared Redis.
func TestReplicationClientReconnectIntegration(t *testing.T) {
	if os.Getenv("APPMANA_REDIS_RECONNECT_TEST") != "1" {
		t.Skip("requires an explicitly selected disposable Redis container")
	}
	expectedID := os.Getenv("APPMANA_REDIS_RECONNECT_RUN_ID")
	require.Len(t, expectedID, 40)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", ContextTimeoutEnabled: true})
	defer client.Close()
	info, err := client.Info(ctx, "Server").Result()
	require.NoError(t, err)
	require.True(t, strings.Contains(info, "\r\nrun_id:"+expectedID+"\r\n"))
	require.Contains(t, info, "redis_version:7.0.15\r\n")
	role, err := checkRedisServerRole(ctx, client, "disposable fixture")
	require.NoError(t, err)
	require.Equal(t, "master", role)
	require.NoError(t, client.Set(ctx, "appmana:reconnect:retained", "preserved", 0).Err())
	observer := client.Conn()
	defer func() { _ = observer.Close() }()
	initial, err := observer.ClientID(ctx).Result()
	require.NoError(t, err)
	// Documentation-only IP, unreachable in the container's empty network.
	require.NoError(t, reconfigureReplicationClient(ctx, client, "192.0.2.1"))
	_, err = observer.ClientID(ctx).Result()
	require.Error(t, err, "demotion left the old socket attached")
	require.NoError(t, observer.Close())
	observer = client.Conn()
	afterDemotion, err := observer.ClientID(ctx).Result()
	require.NoError(t, err)
	require.NotEqual(t, initial, afterDemotion, "demotion left the old socket attached")
	require.NoError(t, promoteReplicationClient(ctx, client))
	_, err = observer.ClientID(ctx).Result()
	require.Error(t, err, "promotion left the old socket attached")
	require.NoError(t, observer.Close())
	observer = client.Conn()
	afterPromotion, err := observer.ClientID(ctx).Result()
	require.NoError(t, err)
	require.NotEqual(t, afterDemotion, afterPromotion, "promotion left the old socket attached")
	require.NoError(t, promoteReplicationClient(ctx, client))
	unchanged, err := observer.ClientID(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, afterPromotion, unchanged, "unchanged primary disconnected its clients")
	value, err := observer.Get(ctx, "appmana:reconnect:retained").Result()
	require.NoError(t, err)
	require.Equal(t, "preserved", value)
}
