package redis

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	rediscli "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Run only in the network-isolated disposable container used by
// hack/recovery/check-sentinel-quorum.sh. No existing Sentinel is modified.
func TestSentinelQuorumIntegration(t *testing.T) {
	if os.Getenv("REDIS_SENTINEL_QUORUM_TEST") != "1" {
		t.Skip("requires isolated Redis container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	masterProcess := exec.CommandContext(ctx, "redis-server", "--port", "6379", "--save", "", "--appendonly", "no")
	require.NoError(t, masterProcess.Start())
	t.Cleanup(func() { _ = masterProcess.Process.Kill(); _ = masterProcess.Wait() })
	masterService := NewClient().Connect(&ConnectionInfo{Host: "127.0.0.1", Port: "6379"})
	require.Eventually(t, func() bool {
		isMaster, err := masterService.IsMaster(ctx)
		return err == nil && isMaster
	}, 5*time.Second, 50*time.Millisecond)
	attached, err := masterService.GetAttachedReplicaCount(ctx)
	require.NoError(t, err)
	require.Zero(t, attached)
	config := filepath.Join(t.TempDir(), "sentinel.conf")
	require.NoError(t, os.WriteFile(config, []byte("port 26379\nbind 127.0.0.1\nsentinel monitor upgrade 127.0.0.1 6379 3\nsentinel down-after-milliseconds upgrade 23456\n"), 0o600))
	process := exec.CommandContext(ctx, "redis-server", config, "--sentinel")
	require.NoError(t, process.Start())
	t.Cleanup(func() { _ = process.Process.Kill(); _ = process.Wait() })
	db := rediscli.NewClient(&rediscli.Options{Addr: "127.0.0.1:26379", MaxRetries: -1})
	t.Cleanup(func() { _ = db.Close() })
	require.Eventually(t, func() bool { return db.Ping(ctx).Err() == nil }, 5*time.Second, 50*time.Millisecond)
	id, err := db.Do(ctx, "SENTINEL", "MYID").Text()
	require.NoError(t, err)
	require.NoError(t, db.Do(ctx, "SENTINEL", "CONFIG", "SET", "resolve-hostnames", "yes").Err())
	svc := NewClient().Connect(&ConnectionInfo{Host: "127.0.0.1", Port: "26379"})
	err = svc.SentinelMonitor(ctx, &ConnectionInfo{Host: "missing.invalid", Port: "6379"}, "upgrade", "2")
	require.ErrorContains(t, err, "resolve replacement Sentinel master")
	retained := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", "upgrade")
	require.NoError(t, db.Process(ctx, retained))
	require.Equal(t, "127.0.0.1", retained.Val()["ip"])
	require.Equal(t, "3", retained.Val()["quorum"])
	require.Equal(t, "23456", retained.Val()["down-after-milliseconds"])
	// Invalid replacement parameters must not erase the existing monitor.
	for _, replacement := range []struct {
		port, quorum string
	}{{"6379", "0"}, {"0", "2"}} {
		svc := NewClient().Connect(&ConnectionInfo{Host: "127.0.0.1", Port: "26379"})
		require.Error(t, svc.SentinelMonitor(ctx, &ConnectionInfo{Host: "127.0.0.2", Port: replacement.port}, "upgrade", replacement.quorum))
		cmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", "upgrade")
		require.NoError(t, db.Process(ctx, cmd), "invalid replacement must preserve the monitor")
		require.Equal(t, "127.0.0.1", cmd.Val()["ip"])
		require.Equal(t, "3", cmd.Val()["quorum"])
		require.Equal(t, "23456", cmd.Val()["down-after-milliseconds"])
	}
	for _, quorum := range []string{"2", "1", "2"} {
		svc := NewClient().Connect(&ConnectionInfo{Host: "127.0.0.1", Port: "26379"})
		require.NoError(t, svc.SentinelMonitor(ctx, &ConnectionInfo{Host: "127.0.0.1", Port: "6379"}, "upgrade", quorum))
		cmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", "upgrade")
		require.NoError(t, db.Process(ctx, cmd))
		require.Equal(t, quorum, cmd.Val()["quorum"])
		require.Equal(t, "23456", cmd.Val()["down-after-milliseconds"], "monitor must not be recreated")
		after, err := db.Do(ctx, "SENTINEL", "MYID").Text()
		require.NoError(t, err)
		require.Equal(t, id, after)
	}
}
