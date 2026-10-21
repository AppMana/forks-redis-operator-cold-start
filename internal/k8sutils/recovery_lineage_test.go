package k8sutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryLineageSamplesCandidateAfterAdvancingReplicas(t *testing.T) {
	var observed []string
	offset := 100
	err := observeRecoveryLineage([]string{"r-0", "r-1", "r-2"}, "r-0", map[string]int64{"r-0": 1, "r-1": 1, "r-2": 1}, func(pod string) (string, error) {
		observed = append(observed, pod)
		// A healthy shared history advances between network round trips.
		offset++
		return fmt.Sprintf("master_replid:abc\r\nmaster_repl_offset:%d\r\n", offset), nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"r-1", "r-2", "r-0"}, observed)
}

func TestRecoveryLineageObservationStillFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name        string
		candidate   string
		counts      map[string]int64
		primaryInfo string
		readError   bool
	}{
		{"candidate absent", "r-9", map[string]int64{"r-0": 1, "r-1": 1, "r-9": 1}, "", false},
		{"unknown peer", "r-0", map[string]int64{"r-0": 1}, "", false},
		{"empty candidate", "r-0", map[string]int64{"r-0": 0, "r-1": 1}, "", false},
		{"lagging candidate", "r-0", map[string]int64{"r-0": 1, "r-1": 1}, "master_replid:abc\r\nmaster_repl_offset:99\r\n", false},
		{"divergent candidate", "r-0", map[string]int64{"r-0": 1, "r-1": 1}, "master_replid:def\r\nmaster_repl_offset:200\r\n", false},
		{"unreachable peer", "r-0", map[string]int64{"r-0": 1, "r-1": 1}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := observeRecoveryLineage([]string{"r-0", "r-1"}, tc.candidate, tc.counts, func(pod string) (string, error) {
				if tc.readError {
					return "", fmt.Errorf("unreachable")
				}
				if pod == tc.candidate {
					return tc.primaryInfo, nil
				}
				return "master_replid:abc\r\nmaster_repl_offset:100\r\n", nil
			})
			require.Error(t, err)
		})
	}
}

func TestRecoveryLineage(t *testing.T) {
	info := func(id string, offset int) string {
		return fmt.Sprintf("master_replid:%s\r\nmaster_repl_offset:%d\r\n", id, offset)
	}
	for _, tc := range []struct {
		name   string
		counts map[string]int64
		infos  map[string]string
		safe   bool
	}{
		{"fresh empty cluster", map[string]int64{"r-0": 0, "r-1": 0}, nil, true},
		{"empty replacement cannot erase survivor", map[string]int64{"r-0": 0, "r-1": 4}, nil, false},
		{"unknown peer is not empty", map[string]int64{"r-0": 4}, nil, false},
		{"same history caught up", map[string]int64{"r-0": 4, "r-1": 4}, map[string]string{"r-0": info("abc", 100), "r-1": info("abc", 100)}, true},
		{"candidate behind", map[string]int64{"r-0": 4, "r-1": 4}, map[string]string{"r-0": info("abc", 99), "r-1": info("abc", 100)}, false},
		{"divergent histories cannot compare offsets", map[string]int64{"r-0": 4, "r-1": 4}, map[string]string{"r-0": info("abc", 200), "r-1": info("def", 100)}, false},
		{"missing lineage fails closed", map[string]int64{"r-0": 4, "r-1": 4}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRecoveryLineage([]string{"r-0", "r-1"}, "r-0", tc.counts, tc.infos)
			if tc.safe {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
