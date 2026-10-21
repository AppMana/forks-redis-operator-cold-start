package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSentinelConfigEscapesPasswords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	t.Setenv("SENTINEL_CONFIG_FILE", path)
	t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "false")
	t.Setenv("MASTER_GROUP_NAME", "test")
	t.Setenv("REDIS_PASSWORD", " own\"quote\\slash \r\n")
	t.Setenv("MASTER_PASSWORD", " master\"quote\\slash \r\n")
	require.NoError(t, GenerateConfig())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), `requirepass " own\"quote\\slash "`)
	require.Contains(t, string(raw), `sentinel auth-pass test " master\"quote\\slash "`)
}

func TestPersistentSentinelRetainsElectionAndRefreshesCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	t.Setenv("SENTINEL_CONFIG_FILE", path)
	t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
	t.Setenv("MASTER_GROUP_NAME", "test")
	t.Setenv("RESOLVE_HOSTNAMES", "yes")
	t.Setenv("MASTER_PASSWORD", "new-test-password")
	old := "sentinel monitor test redis-2.redis-headless.ns.svc.cluster.local 6379 2\nsentinel myid abcdef\nsentinel current-epoch 12\nsentinel config-epoch test 11\nsentinel leader-epoch test 12\nsentinel known-replica test redis-1.redis-headless.ns.svc.cluster.local 6379\nsentinel auth-pass test old-test-password\n"
	require.NoError(t, os.WriteFile(path, []byte(old), 0o600))
	require.NoError(t, GenerateConfig())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	got := string(raw)
	require.Contains(t, got, "sentinel monitor test redis-2.redis-headless.ns.svc.cluster.local 6379 2")
	require.Contains(t, got, "sentinel current-epoch 12")
	require.Contains(t, got, "sentinel myid abcdef")
	require.Contains(t, got, "sentinel config-epoch test 11")
	require.Contains(t, got, "sentinel auth-pass test new-test-password")
	require.NotContains(t, got, "old-test-password")
	require.Less(t, strings.Index(got, "sentinel resolve-hostnames yes"), strings.Index(got, "sentinel monitor test"))
	require.NoError(t, GenerateConfig())
	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, got, string(again))
}

func TestPersistentSentinelRejectsUnusableStateWithoutOverwriting(t *testing.T) {
	for _, content := range []string{"", "sentinel current-epoch 12\n", "sentinel monitor other 10.0.0.1 6379 2\n"} {
		t.Run(content, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sentinel.conf")
			t.Setenv("SENTINEL_CONFIG_FILE", path)
			t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
			t.Setenv("MASTER_GROUP_NAME", "test")
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			require.Error(t, GenerateConfig())
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, content, string(got))
		})
	}
}

func TestPersistentSentinelFirstBoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	t.Setenv("SENTINEL_CONFIG_FILE", path)
	t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
	require.NoError(t, GenerateConfig())
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestPersistentSentinelNormalizesRetainedReplicaInventory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	t.Setenv("SENTINEL_CONFIG_FILE", path)
	t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
	t.Setenv("MASTER_GROUP_NAME", "test")
	old := "sentinel monitor test redis-2.example 6379 2\n" +
		"sentinel myid retained-id\nsentinel current-epoch 12\nsentinel config-epoch test 11\nsentinel leader-epoch test 12\n" +
		"sentinel known-replica test redis-2.example 6379\n" +
		"sentinel known-replica test redis-2.example 6379\n" +
		"sentinel known-slave test redis-1.example 6379\n" +
		"sentinel known-replica test redis-1.example 6379\n" +
		"sentinel known-replica test redis-1.example 6380\n" +
		"sentinel known-sentinel test sentinel-1.example 26379 other-id\n"
	require.NoError(t, os.WriteFile(path, []byte(old), 0o600))
	require.NoError(t, GenerateConfig())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	got := string(raw)
	require.NotContains(t, got, "sentinel known-replica test redis-2.example")
	require.Equal(t, 1, strings.Count(got, "test redis-1.example 6379"))
	require.Contains(t, got, "sentinel known-replica test redis-1.example 6380")
	for _, line := range []string{"sentinel myid retained-id", "sentinel current-epoch 12", "sentinel config-epoch test 11", "sentinel leader-epoch test 12", "sentinel known-sentinel test sentinel-1.example 26379 other-id"} {
		require.Contains(t, got, line)
	}
	require.NoError(t, GenerateConfig())
	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, got, string(again))
}

func TestPersistentSentinelKeepsResolverUntilHostnameStateIsMigrated(t *testing.T) {
	for _, state := range []string{
		"sentinel monitor test localhost 6379 2\n",
		"sentinel monitor test 127.0.0.1 6379 2\nsentinel known-replica test localhost 6379\n",
		"sentinel monitor test 127.0.0.1 6379 2\nsentinel known-slave test localhost 6379\n",
		"sentinel monitor test 127.0.0.1 6379 2\nsentinel known-sentinel test localhost 26379 abcdef\n",
	} {
		t.Run(state, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sentinel.conf")
			t.Setenv("SENTINEL_CONFIG_FILE", path)
			t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
			t.Setenv("MASTER_GROUP_NAME", "test")
			t.Setenv("RESOLVE_HOSTNAMES", "no")
			t.Setenv("ANNOUNCE_HOSTNAMES", "no")
			require.NoError(t, os.WriteFile(path, []byte(state), 0o600))
			require.NoError(t, GenerateConfig())
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Contains(t, string(got), "sentinel resolve-hostnames yes")
			require.Contains(t, string(got), "sentinel announce-hostnames no")
			for _, line := range strings.Split(strings.TrimSpace(state), "\n") {
				require.Contains(t, string(got), line)
			}
			// Once the controller has replaced DNS monitor/topology with IPs,
			// subsequent restarts can honor resolver=no without losing state.
			require.NoError(t, os.WriteFile(path, []byte("sentinel monitor test 127.0.0.1 6379 2\nsentinel known-replica test ::1 6379\n"), 0o600))
			require.NoError(t, GenerateConfig())
			got, err = os.ReadFile(path)
			require.NoError(t, err)
			require.Contains(t, string(got), "sentinel resolve-hostnames no")
		})
	}
}
