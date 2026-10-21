package redis

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	rediscli "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Interrupt after the real Sentinel has committed REMOVE, before the caller
// can send MONITOR. Only installed on a disposable integration-test client.
type interruptMonitorRemoval struct{}

func (interruptMonitorRemoval) DialHook(next rediscli.DialHook) rediscli.DialHook {
	return next
}

func (interruptMonitorRemoval) ProcessPipelineHook(next rediscli.ProcessPipelineHook) rediscli.ProcessPipelineHook {
	return next
}

func (interruptMonitorRemoval) ProcessHook(next rediscli.ProcessHook) rediscli.ProcessHook {
	return func(ctx context.Context, cmd rediscli.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		if err == nil && len(args) > 1 && strings.EqualFold(cmd.Name(), "sentinel") && strings.EqualFold(args[1].(string), "remove") {
			return errors.New("injected interruption after committed REMOVE")
		}
		return err
	}
}

func TestSentinelMigrationInterruptionIntegration(t *testing.T) {
	if os.Getenv("REDIS_SENTINEL_QUORUM_TEST") != "1" {
		t.Skip("requires isolated Redis container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	start := func(args ...string) *exec.Cmd {
		process := exec.CommandContext(ctx, "redis-server", args...)
		require.NoError(t, process.Start())
		t.Cleanup(func() { _ = process.Process.Kill(); _ = process.Wait() })
		return process
	}
	connect := func(port string) *rediscli.Client {
		db := rediscli.NewClient(&rediscli.Options{Addr: "127.0.0.1:" + port, MaxRetries: -1, ContextTimeoutEnabled: true})
		t.Cleanup(func() { _ = db.Close() })
		require.Eventually(t, func() bool { return db.Ping(ctx).Err() == nil }, 5*time.Second, 25*time.Millisecond)
		return db
	}
	var masters []*rediscli.Client
	for _, port := range []string{"6379", "6380"} {
		start("--port", port, "--save", "", "--appendonly", "no")
		db := connect(port)
		require.NoError(t, db.Set(ctx, "retained", "data-on-"+port, 0).Err())
		masters = append(masters, db)
	}
	var sentinels []*rediscli.Client
	var sentinelProcesses []*exec.Cmd
	var configs, identities []string
	for i := 0; i < 3; i++ {
		port := strconv.Itoa(26379 + i)
		config := filepath.Join(t.TempDir(), "sentinel.conf")
		require.NoError(t, os.WriteFile(config, []byte("port "+port+"\nbind 127.0.0.1\nsentinel monitor migration 127.0.0.1 6379 2\n"), 0o600))
		sentinelProcesses = append(sentinelProcesses, start(config, "--sentinel"))
		db := connect(port)
		id, err := db.Do(ctx, "SENTINEL", "MYID").Text()
		require.NoError(t, err)
		sentinels = append(sentinels, db)
		configs = append(configs, config)
		identities = append(identities, id)
	}
	var mutations []string
	makePeers := func(interrupt bool) []SentinelMonitorPeer {
		var peers []SentinelMonitorPeer
		for i := 0; i < 3; i++ {
			i := i
			port := strconv.Itoa(26379 + i)
			observe := NewClient().Connect(&ConnectionInfo{Host: "127.0.0.1", Port: port})
			peers = append(peers, SentinelMonitorPeer{
				Name:    port,
				Healthy: func(ctx context.Context) (bool, error) { return observe.SentinelMonitorHealthy(ctx, "migration") },
				Reconcile: func(ctx context.Context) error {
					mutations = append(mutations, port)
					svc := NewClient().Connect(&ConnectionInfo{Host: "127.0.0.1", Port: port})
					if interrupt && i == 1 {
						db := rediscli.NewClient(&rediscli.Options{Addr: "127.0.0.1:" + port, MaxRetries: -1, ContextTimeoutEnabled: true})
						db.AddHook(interruptMonitorRemoval{})
						svc = &service{redisClient: db}
					}
					return svc.SentinelMonitor(ctx, &ConnectionInfo{Host: "127.0.0.1", Port: "6380"}, "migration", "2")
				},
			})
		}
		return peers
	}
	for _, peer := range makePeers(false) {
		require.Eventually(t, func() bool { healthy, err := peer.Healthy(ctx); return err == nil && healthy }, 5*time.Second, 25*time.Millisecond)
	}
	require.ErrorContains(t, ReconcileSentinelMonitorBatch(ctx, makePeers(true)), "injected interruption")
	require.Equal(t, []string{"26379", "26380"}, mutations)
	monitor := func(i int) map[string]string {
		cmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", "migration")
		require.NoError(t, sentinels[i].Process(ctx, cmd))
		return cmd.Val()
	}
	require.Equal(t, "6380", monitor(0)["port"])
	require.ErrorContains(t, sentinels[1].Do(ctx, "SENTINEL", "MASTER", "migration").Err(), "No such master")
	require.Equal(t, "6379", monitor(2)["port"], "untouched peer retains the old target")
	// All service/coordinator objects are fresh; remote Sentinel state alone
	// must identify the interrupted peer before changing any healthy monitor.
	mutations = nil
	require.ErrorContains(t, ReconcileSentinelMonitorBatch(ctx, makePeers(false)), "waiting to verify repaired Sentinel 26380")
	require.Equal(t, []string{"26380"}, mutations)
	require.Eventually(t, func() bool {
		healthy, err := makePeers(false)[1].Healthy(ctx)
		return err == nil && healthy
	}, 5*time.Second, 25*time.Millisecond)
	require.NoError(t, ReconcileSentinelMonitorBatch(ctx, makePeers(false)))
	for i, db := range sentinels {
		require.Equal(t, "6380", monitor(i)["port"])
		id, err := db.Do(ctx, "SENTINEL", "MYID").Text()
		require.NoError(t, err)
		require.Equal(t, identities[i], id)
		persisted, err := os.ReadFile(configs[i])
		require.NoError(t, err)
		require.Contains(t, string(persisted), "sentinel monitor migration 127.0.0.1 6380 2")
	}
	// Crash and reopen the actual Sentinel processes from their rewritten
	// configs: an API-only repair that was never persisted is insufficient.
	for _, process := range sentinelProcesses {
		require.NoError(t, process.Process.Kill())
	}
	for _, process := range sentinelProcesses {
		_ = process.Wait()
	}
	for i, config := range configs {
		start(config, "--sentinel")
		sentinels[i] = connect(strconv.Itoa(26379 + i))
	}
	for i, peer := range makePeers(false) {
		require.Eventually(t, func() bool { healthy, err := peer.Healthy(ctx); return err == nil && healthy }, 5*time.Second, 25*time.Millisecond)
		require.Equal(t, "6380", monitor(i)["port"])
		id, err := sentinels[i].Do(ctx, "SENTINEL", "MYID").Text()
		require.NoError(t, err)
		require.Equal(t, identities[i], id)
	}
	for i, db := range masters {
		value, err := db.Get(ctx, "retained").Result()
		require.NoError(t, err)
		require.Equal(t, "data-on-"+strconv.Itoa(6379+i), value)
	}
}
