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
			cr:         mk("redis-repl-1"),
			candidates: []string{"redis-repl-0", "redis-repl-1", "redis-repl-2"},
			want:       "redis-repl-1",
		},
		{
			name:       "status points at non-candidate, fall back to lowest ordinal",
			cr:         mk("redis-repl-5"),
			candidates: []string{"redis-repl-0", "redis-repl-1"},
			want:       "redis-repl-0",
		},
		{
			name:       "fresh cold start: no status, pick lowest ordinal",
			cr:         mk(""),
			candidates: []string{"redis-repl-0", "redis-repl-1", "redis-repl-2"},
			want:       "redis-repl-0",
		},
		{
			name:       "nil CR, pick lowest ordinal",
			cr:         nil,
			candidates: []string{"redis-repl-0", "redis-repl-1"},
			want:       "redis-repl-0",
		},
		{
			name:       "no candidates, return empty",
			cr:         mk(""),
			candidates: []string{},
			want:       "",
		},
		{
			name:       "candidates are the slave pods (all-slaves cold start)",
			cr:         mk("redis-repl-0"),
			candidates: []string{"redis-repl-0", "redis-repl-1", "redis-repl-2"},
			want:       "redis-repl-0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PickPreferredMaster(tc.cr, tc.candidates)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseReplicationNodeInfo(t *testing.T) {
	info, err := ParseReplicationNodeInfo(`# Replication
role:slave
master_host:10.3.239.142
master_port:6379
master_link_status:down
slave_repl_offset:421168632
connected_slaves:0
master_replid:abc123def456
master_replid2:0000000000000000000000000000000000000000
master_repl_offset:421168632
second_repl_offset:-1
`)
	assert.NoError(t, err)
	assert.Equal(t, "slave", info.Role)
	assert.Equal(t, "10.3.239.142", info.MasterHost)
	assert.Equal(t, "down", info.MasterLinkStatus)
	assert.Equal(t, int64(421168632), info.SlaveReplOffset)
	assert.Equal(t, int64(421168632), info.MasterReplOffset)
	assert.Equal(t, int64(421168632), info.EffectiveOffset())
}

func TestSelectRecoveryMasterNeverPicksEmptyRestartedMasterOverHighOffsetSlaves(t *testing.T) {
	cr := &rrvb2.RedisReplication{
		Status: rrvb2.RedisReplicationStatus{MasterNode: "redis-master-2"},
	}
	masters := []string{"redis-master-2"}
	slaves := []string{"redis-master-0", "redis-master-1"}
	counts := map[string]int64{
		"redis-master-0": 2,
		"redis-master-1": 2,
		"redis-master-2": 0,
	}
	infos := map[string]ReplicationNodeInfo{
		"redis-master-0": {
			Role:             "slave",
			MasterHost:       "10.3.239.142",
			MasterLinkStatus: "down",
			SlaveReplOffset:  421168632,
			MasterReplOffset: 421168632,
		},
		"redis-master-1": {
			Role:             "slave",
			MasterHost:       "10.3.239.142",
			MasterLinkStatus: "down",
			SlaveReplOffset:  421168632,
			MasterReplOffset: 421168632,
		},
		"redis-master-2": {
			Role:             "master",
			MasterReplOffset: 0,
		},
	}

	got := SelectRecoveryMaster(cr, masters, slaves, counts, infos)
	assert.Equal(t, "redis-master-0", got)

	// When redis-master-1 has a strictly higher offset than redis-master-0,
	// SelectRecoveryMaster deterministically chooses redis-master-1.
	infos["redis-master-1"] = ReplicationNodeInfo{
		Role:             "slave",
		MasterHost:       "10.3.239.142",
		MasterLinkStatus: "down",
		SlaveReplOffset:  421169999,
		MasterReplOffset: 421169999,
	}
	got = SelectRecoveryMaster(cr, masters, slaves, counts, infos)
	assert.Equal(t, "redis-master-1", got)
}
