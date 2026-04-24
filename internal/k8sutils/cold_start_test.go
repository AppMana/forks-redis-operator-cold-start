package k8sutils

import (
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/stretchr/testify/assert"
)

func TestPickPreferredMaster(t *testing.T) {
	mk := func(masterNode string) *rrvb2.RedisReplication {
		return &rrvb2.RedisReplication{Status: rrvb2.RedisReplicationStatus{MasterNode: masterNode}}
	}
	tests := []struct {
		name       string
		cr         *rrvb2.RedisReplication
		candidates []string
		want       string
	}{
		{
			name:       "status points at candidate (split-brain recovery)",
			cr:         mk("redis-harbor-1"),
			candidates: []string{"redis-harbor-0", "redis-harbor-1", "redis-harbor-2"},
			want:       "redis-harbor-1",
		},
		{
			name:       "status points at non-candidate, fall back to lowest ordinal",
			cr:         mk("redis-harbor-5"),
			candidates: []string{"redis-harbor-0", "redis-harbor-1"},
			want:       "redis-harbor-0",
		},
		{
			name:       "fresh cold start: no status, pick lowest ordinal",
			cr:         mk(""),
			candidates: []string{"redis-harbor-0", "redis-harbor-1", "redis-harbor-2"},
			want:       "redis-harbor-0",
		},
		{
			name:       "nil CR, pick lowest ordinal",
			cr:         nil,
			candidates: []string{"redis-lago-0", "redis-lago-1"},
			want:       "redis-lago-0",
		},
		{
			name:       "no candidates, return empty",
			cr:         mk(""),
			candidates: []string{},
			want:       "",
		},
		{
			name:       "candidates are the slave pods (all-slaves cold start)",
			cr:         mk("redis-harbor-0"),
			candidates: []string{"redis-harbor-0", "redis-harbor-1", "redis-harbor-2"},
			want:       "redis-harbor-0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PickPreferredMaster(tc.cr, tc.candidates)
			assert.Equal(t, tc.want, got)
		})
	}
}
