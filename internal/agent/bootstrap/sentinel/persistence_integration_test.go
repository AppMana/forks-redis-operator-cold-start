package bootstrap

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	rediscli "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSentinelRetainedReplicaRewriteIntegration(t *testing.T) {
	if os.Getenv("REDIS_SENTINEL_PERSISTENCE_TEST") != "1" {
		t.Skip("requires isolated Redis container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	const identity = "0123456789012345678901234567890123456789"
	// During a simultaneous pod restart, all three distinct hostnames may be
	// unresolved. Redis's address comparison can then treat their empty resolved
	// IPs as equal and rewrite both replicas to the monitored primary hostname.
	config := "port 26379\nbind 127.0.0.1\n" +
		"sentinel resolve-hostnames yes\nsentinel announce-hostnames yes\n" +
		"sentinel monitor test primary.invalid 6379 2\n" +
		"sentinel myid " + identity + "\n" +
		"sentinel current-epoch 12\nsentinel config-epoch test 11\nsentinel leader-epoch test 12\n" +
		"sentinel known-replica test replica-0.invalid 6379\n" +
		"sentinel known-replica test replica-1.invalid 6379\n"
	require.NoError(t, os.WriteFile(path, []byte(config), 0o600))
	start := func() *exec.Cmd {
		process := exec.CommandContext(ctx, "redis-server", path, "--sentinel")
		require.NoError(t, process.Start())
		t.Cleanup(func() { _ = process.Process.Kill(); _ = process.Wait() })
		return process
	}
	connect := func() *rediscli.Client {
		db := rediscli.NewClient(&rediscli.Options{Addr: "127.0.0.1:26379", MaxRetries: -1, ContextTimeoutEnabled: true})
		t.Cleanup(func() { _ = db.Close() })
		require.Eventually(t, func() bool { return db.Ping(ctx).Err() == nil }, 3*time.Second, 25*time.Millisecond)
		return db
	}
	process := start()
	db := connect()
	require.NoError(t, db.Do(ctx, "SENTINEL", "FLUSHCONFIG").Err())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(raw), "sentinel known-replica test primary.invalid 6379"), "real Redis rewrite must reproduce the duplicate")
	require.NoError(t, process.Process.Kill())
	_ = process.Wait()
	output, err := exec.CommandContext(ctx, "redis-server", path, "--sentinel").CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "Duplicate hostname and port for replica")
	t.Setenv("SENTINEL_CONFIG_FILE", path)
	t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
	t.Setenv("MASTER_GROUP_NAME", "test")
	t.Setenv("RESOLVE_HOSTNAMES", "yes")
	t.Setenv("ANNOUNCE_HOSTNAMES", "yes")
	require.NoError(t, GenerateConfig())
	start()
	repaired := connect()
	id, err := repaired.Do(ctx, "SENTINEL", "MYID").Text()
	require.NoError(t, err)
	require.Equal(t, identity, id)
	master := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", "test")
	require.NoError(t, repaired.Process(ctx, master))
	require.Equal(t, "primary.invalid", master.Val()["ip"])
	require.Equal(t, "11", master.Val()["config-epoch"])
	require.Equal(t, "0", master.Val()["num-slaves"])
	raw, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), "sentinel current-epoch 12")
	require.Contains(t, string(raw), "sentinel leader-epoch test 12")
}
