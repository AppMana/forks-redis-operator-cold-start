package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistentSentinelRetainsElectionAndRefreshesCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sentinel.conf")
	t.Setenv("SENTINEL_CONFIG_FILE", path)
	t.Setenv("SENTINEL_PERSISTENCE_ENABLED", "true")
	t.Setenv("MASTER_GROUP_NAME", "test")
	t.Setenv("RESOLVE_HOSTNAMES", "yes")
	t.Setenv("MASTER_PASSWORD", "new-test-password")
	old := "sentinel monitor test redis-2.redis-headless.ns.svc.cluster.local 6379 2\nsentinel myid abcdef\nsentinel current-epoch 12\nsentinel config-epoch test 11\nsentinel leader-epoch test 12\nsentinel known-replica test redis-1.redis-headless.ns.svc.cluster.local 6379\nsentinel auth-pass test old-test-password\n"
	require.NoError(t, os.WriteFile(path, []byte(old), 0600))
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
			require.NoError(t, os.WriteFile(path, []byte(content), 0600))
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
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}
