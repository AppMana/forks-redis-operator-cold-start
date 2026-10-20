package k8sutils

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

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
