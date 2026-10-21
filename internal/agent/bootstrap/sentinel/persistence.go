package bootstrap

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

func retainedElectionNeedsResolver(monitor []string, state [][]string) bool {
	if len(monitor) >= 4 && net.ParseIP(monitor[3]) == nil {
		return true
	}
	for _, fields := range state {
		if len(fields) < 4 {
			continue
		}
		switch fields[1] {
		case "known-replica", "known-slave", "known-sentinel":
			if net.ParseIP(fields[3]) == nil {
				return true
			}
		}
	}
	return false
}

// Keep only Sentinel-owned election state. Desired credentials, TLS and tuning
// are regenerated, so a retained config never prevents credential rotation.
func retainedElection(path, group string) (monitor []string, state [][]string, err error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read retained Sentinel configuration: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || strings.ToLower(fields[0]) != "sentinel" {
			continue
		}
		fields[0] = "sentinel"
		fields[1] = strings.ToLower(fields[1])
		switch fields[1] {
		case "monitor":
			if len(fields) != 6 || fields[2] != group || monitor != nil {
				return nil, nil, fmt.Errorf("retained Sentinel monitor does not match the configured group")
			}
			monitor = fields
		case "myid", "current-epoch":
			if len(fields) != 3 {
				return nil, nil, fmt.Errorf("invalid retained Sentinel identity or epoch")
			}
			state = append(state, fields)
		case "config-epoch", "leader-epoch", "known-replica", "known-slave", "known-sentinel":
			expected := 4
			if fields[1] == "known-replica" || fields[1] == "known-slave" {
				expected = 5
			}
			if fields[1] == "known-sentinel" {
				expected = 6
			}
			if len(fields) != expected || fields[2] != group {
				return nil, nil, fmt.Errorf("invalid retained Sentinel topology")
			}
			state = append(state, fields)
		}
	}
	if monitor == nil {
		return nil, nil, fmt.Errorf("retained Sentinel configuration has no monitor; refusing to overwrite election state")
	}
	// Discovered replicas are advisory topology, not election identity. Redis
	// refuses to start with duplicate replica endpoints in a rewritten config.
	// A former replica may also now be the retained monitored primary. Keep the
	// monitor and all election/peer state, but emit each other replica once.
	// Compare literal endpoints only: DNS is not reliable during cold startup.
	seenReplicas := map[string]bool{net.JoinHostPort(monitor[3], monitor[4]): true}
	filtered := make([][]string, 0, len(state))
	for _, fields := range state {
		if fields[1] == "known-replica" || fields[1] == "known-slave" {
			endpoint := net.JoinHostPort(fields[3], fields[4])
			if seenReplicas[endpoint] {
				continue
			}
			seenReplicas[endpoint] = true
		}
		filtered = append(filtered, fields)
	}
	return monitor, filtered, nil
}
