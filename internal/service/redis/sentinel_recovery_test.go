package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestSentinelMonitorHealthRequiresObservedProcess(t *testing.T) {
	for _, tc := range []struct {
		name, flags, runid string
		healthy            bool
	}{
		{"connected", "master", "observed-process", true},
		{"new monitor", "master", "", false},
		{"disconnected", "master,disconnected", "observed-process", false},
		{"down", "master,s_down", "observed-process", false},
		{"failover", "master,failover_in_progress", "observed-process", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, m := redismock.NewClientMock()
			m.ExpectDo("SENTINEL", "MASTER", "group").SetVal(map[string]string{"ip": "127.0.0.1", "port": "6379", "flags": tc.flags, "runid": tc.runid, "role-reported": "master"})
			healthy, err := (&service{redisClient: c}).SentinelMonitorHealthy(context.Background(), "group")
			require.NoError(t, err)
			require.Equal(t, tc.healthy, healthy)
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
	for _, failure := range []error{sentinelTestError("ERR No such master with that name"), errors.New("unreachable")} {
		c, m := redismock.NewClientMock()
		m.ExpectDo("SENTINEL", "MASTER", "group").SetErr(failure)
		healthy, err := (&service{redisClient: c}).SentinelMonitorHealthy(context.Background(), "group")
		require.False(t, healthy)
		if _, missing := failure.(sentinelTestError); missing {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		require.NoError(t, m.ExpectationsWereMet())
	}
}

func TestSentinelBatchRepairsInterruptedMonitorBeforeHealthyPeers(t *testing.T) {
	// Model remote persisted state surviving loss of every controller object.
	healthy := map[string]bool{"a": true, "b": true, "c": true}
	var mutations []string
	fail := true
	makePeers := func() []SentinelMonitorPeer {
		var peers []SentinelMonitorPeer
		for _, name := range []string{"a", "b", "c"} {
			name := name
			peers = append(peers, SentinelMonitorPeer{
				Name:    name,
				Healthy: func(context.Context) (bool, error) { return healthy[name], nil },
				Reconcile: func(context.Context) error {
					mutations = append(mutations, name)
					if name == "b" && fail {
						healthy[name] = false // REMOVE committed, MONITOR interrupted.
						return errors.New("connection lost")
					}
					healthy[name] = true
					return nil
				},
			})
		}
		return peers
	}
	require.ErrorContains(t, ReconcileSentinelMonitorBatch(context.Background(), makePeers()), "connection lost")
	require.Equal(t, []string{"a", "b"}, mutations)
	mutations = nil
	require.ErrorContains(t, ReconcileSentinelMonitorBatch(context.Background(), makePeers()), "connection lost")
	require.Equal(t, []string{"b"}, mutations, "fresh coordinator must repair b before touching a or c")
	fail = false
	mutations = nil
	require.ErrorContains(t, ReconcileSentinelMonitorBatch(context.Background(), makePeers()), "waiting to verify")
	require.Equal(t, []string{"b"}, mutations)
	mutations = nil
	require.NoError(t, ReconcileSentinelMonitorBatch(context.Background(), makePeers()))
	require.Equal(t, []string{"a", "b", "c"}, mutations)
}

func TestSentinelBatchWaitsForObservedHealthAfterAcknowledgement(t *testing.T) {
	for _, observationError := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		var calls []string
		changed := false
		peers := []SentinelMonitorPeer{
			{Name: "a", Healthy: func(context.Context) (bool, error) {
				if changed && observationError {
					return false, errors.New("observation unavailable")
				}
				return !changed, nil
			}, Reconcile: func(context.Context) error { changed = true; calls = append(calls, "a"); return nil }},
			{
				Name: "b", Healthy: func(context.Context) (bool, error) { return true, nil },
				Reconcile: func(context.Context) error { calls = append(calls, "b"); return nil },
			},
		}
		require.Error(t, ReconcileSentinelMonitorBatch(ctx, peers))
		require.Equal(t, []string{"a"}, calls)
	}
}

func TestSentinelBatchReconnectionDoesNotReplayMutation(t *testing.T) {
	var mutations []string
	probes := 0
	peers := []SentinelMonitorPeer{
		{Name: "a", Healthy: func(context.Context) (bool, error) {
			probes++
			return probes == 1 || probes >= 3, nil
		}, Reconcile: func(context.Context) error { mutations = append(mutations, "a"); return nil }},
		{
			Name: "b", Healthy: func(context.Context) (bool, error) { return true, nil },
			Reconcile: func(context.Context) error { mutations = append(mutations, "b"); return nil },
		},
	}
	require.NoError(t, ReconcileSentinelMonitorBatch(context.Background(), peers))
	require.Equal(t, []string{"a", "b"}, mutations)
	require.Equal(t, 4, probes)
}

func TestSentinelBatchObservesWholeInventoryBeforeMutation(t *testing.T) {
	mutate := func(context.Context) error { t.Fatal("must not mutate on incomplete observation"); return nil }
	peers := []SentinelMonitorPeer{
		{Name: "a", Healthy: func(context.Context) (bool, error) { return false, nil }, Reconcile: mutate},
		{Name: "b", Healthy: func(context.Context) (bool, error) { return false, errors.New("unreachable") }, Reconcile: mutate},
	}
	require.Error(t, ReconcileSentinelMonitorBatch(context.Background(), peers))
	require.Error(t, ReconcileSentinelMonitorBatch(context.Background(), nil))
	require.Error(t, ReconcileSentinelMonitorBatch(context.Background(), []SentinelMonitorPeer{peers[0], peers[0]}))
}

func TestSentinelBatchStopsWhenAnotherMonitorDegradesDuringReconciliation(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint("observation-error=", unavailable), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var mutations []string
			changed := false
			var peers []SentinelMonitorPeer
			for _, name := range []string{"a", "b", "c"} {
				name := name
				peers = append(peers, SentinelMonitorPeer{
					Name: name,
					Healthy: func(context.Context) (bool, error) {
						if name == "c" && changed {
							if unavailable {
								return false, errors.New("monitor unreachable")
							}
							return false, nil
						}
						return true, nil
					},
					Reconcile: func(context.Context) error {
						mutations = append(mutations, name)
						changed = true
						return nil
					},
				})
			}
			require.Error(t, ReconcileSentinelMonitorBatch(ctx, peers))
			require.Equal(t, []string{"a"}, mutations, "do not change b using the inventory from before a was reconciled")
		})
	}
}

func TestSentinelBatchCancellationPreventsMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	peer := SentinelMonitorPeer{Name: "a", Healthy: func(context.Context) (bool, error) {
		cancel()
		return false, nil
	}, Reconcile: func(context.Context) error { t.Fatal("cancelled pass must not mutate"); return nil }}
	require.ErrorIs(t, ReconcileSentinelMonitorBatch(ctx, []SentinelMonitorPeer{peer}), context.Canceled)
}

func TestSentinelMonitorHealthRejectsIncompleteAndReplicaObservations(t *testing.T) {
	for _, state := range []map[string]string{
		{},
		{"ip": "127.0.0.1", "port": "invalid", "flags": "master"},
		{"ip": "127.0.0.1", "port": "6379", "flags": "master", "runid": "known", "role-reported": "slave"},
	} {
		c, m := redismock.NewClientMock()
		m.ExpectDo("SENTINEL", "MASTER", "group").SetVal(state)
		healthy, _ := (&service{redisClient: c}).SentinelMonitorHealthy(context.Background(), "group")
		require.False(t, healthy)
		require.NoError(t, m.ExpectationsWereMet())
	}
}
