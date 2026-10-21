package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	rediscli "github.com/redis/go-redis/v9"
)

// SentinelMonitorHealthy requires an observed Redis process and a connected
// monitor. A newly acknowledged MONITOR has no runid and is not yet usable.
func (c *service) SentinelMonitorHealthy(ctx context.Context, group string) (bool, error) {
	client := c.createClient()
	if client == nil {
		return false, errConnectionInfoNotSet
	}
	defer client.Close()
	cmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", group)
	if err := client.Process(ctx, cmd); err != nil {
		if rediscli.HasErrorPrefix(err, "No such master with that name") {
			return false, nil
		}
		return false, fmt.Errorf("observe Sentinel monitor health: %w", err)
	}
	state := cmd.Val()
	port, err := strconv.ParseUint(state["port"], 10, 16)
	if state["ip"] == "" || err != nil || port == 0 || state["flags"] == "" {
		return false, errors.New("sentinel monitor health observation is incomplete")
	}
	if state["runid"] == "" || state["role-reported"] != "master" {
		return false, nil
	}
	for _, flag := range strings.Split(state["flags"], ",") {
		if flag != "master" {
			return false, nil
		}
	}
	return true, nil
}

// SentinelMonitorPeer supplies live observations rather than controller-memory
// progress. A fresh controller must rediscover interrupted replacements first.
type SentinelMonitorPeer struct {
	Name      string
	Healthy   func(context.Context) (bool, error)
	Reconcile func(context.Context) error
}

// ReconcileSentinelMonitorBatch repairs damaged monitors before permitting
// changes to healthy peers. Callers must include every configured peer, and
// Healthy must require a positively observed, connected monitor (not just pod
// readiness or successful MONITOR acknowledgement).
func ReconcileSentinelMonitorBatch(ctx context.Context, peers []SentinelMonitorPeer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(peers) == 0 {
		return errors.New("no Sentinel monitors available for reconciliation")
	}
	seen := map[string]bool{}
	for _, peer := range peers {
		if peer.Name == "" || seen[peer.Name] || peer.Healthy == nil || peer.Reconcile == nil {
			return errors.New("sentinel monitor inventory is incomplete or ambiguous")
		}
		seen[peer.Name] = true
	}
	var unhealthy []int
	for i, peer := range peers {
		if err := ctx.Err(); err != nil {
			return err
		}
		healthy, err := peer.Healthy(ctx)
		if err != nil {
			return fmt.Errorf("observe Sentinel %s before changing monitors: %w", peer.Name, err)
		}
		if !healthy {
			unhealthy = append(unhealthy, i)
		}
	}
	if len(unhealthy) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Even a successfully repaired monitor needs a new observation pass.
		// Do not advance to healthy peers from the pre-repair snapshot.
		peer := peers[unhealthy[0]]
		if err := peer.Reconcile(ctx); err != nil {
			return fmt.Errorf("repair Sentinel %s: %w", peer.Name, err)
		}
		return fmt.Errorf("waiting to verify repaired Sentinel %s", peer.Name)
	}
	for i, peer := range peers {
		// The initial inventory expires as soon as reconciliation starts.
		// A different monitor may fail while this peer's predecessor recovers.
		// Leave repairs to the next full pass instead of changing more peers
		// on the strength of a stale healthy snapshot.
		if i > 0 {
			for _, observed := range peers {
				if err := ctx.Err(); err != nil {
					return err
				}
				healthy, err := observed.Healthy(ctx)
				if err != nil {
					return fmt.Errorf("recheck Sentinel %s before changing %s: %w", observed.Name, peer.Name, err)
				}
				if !healthy {
					return fmt.Errorf("sentinel %s became unhealthy before changing %s", observed.Name, peer.Name)
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := peer.Reconcile(ctx); err != nil {
			return fmt.Errorf("reconcile Sentinel %s: %w", peer.Name, err)
		}
		if err := waitForSentinelMonitorHealth(ctx, peer); err != nil {
			return err
		}
	}
	return nil
}

// A Sentinel monitor can be transiently unhealthy after reconciliation.
// Returning immediately on that transient can repeatedly replay mutations on
// the first peer, delaying the remaining peers. Observe recovery without mutating
// again, with one bounded budget and the caller's cancellation honored.
func waitForSentinelMonitorHealth(ctx context.Context, peer SentinelMonitorPeer) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		healthy, err := peer.Healthy(ctx)
		if err != nil {
			return fmt.Errorf("verify Sentinel %s after reconciliation: %w", peer.Name, err)
		}
		if healthy {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("waiting for Sentinel %s monitor health: %w", peer.Name, ctx.Err())
		case <-timer.C:
		}
	}
}
