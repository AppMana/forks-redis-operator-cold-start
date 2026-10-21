package redis

import (
	"context"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoleObservationRejectsUnknownReplies(t *testing.T) {
	for _, tc := range []struct {
		name, info      string
		master, invalid bool
	}{
		{name: "master", info: "# Replication\r\nrole:master\r\n", master: true},
		{name: "replica", info: "# Replication\r\nrole:slave\r\n"},
		{name: "empty", invalid: true},
		{name: "unknown", info: "role:unknown\r\n", invalid: true},
		{name: "substring", info: "other:role:master\r\n", invalid: true},
		{name: "suffix", info: "role:master-invalid\r\n", invalid: true},
		{name: "duplicate", info: "role:master\r\nrole:slave\r\n", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			mock.ExpectInfo("replication").SetVal(tc.info)
			got, err := (&service{redisClient: db}).IsMaster(context.Background())
			require.Equal(t, tc.invalid, err != nil)
			require.Equal(t, tc.master, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReplicaCountRejectsUnknownReplies(t *testing.T) {
	for _, tc := range []struct {
		name, info string
		count      int
		invalid    bool
	}{
		{name: "zero", info: "connected_slaves:0\r\n"},
		{name: "two", info: "connected_slaves:2\r\n", count: 2},
		{name: "missing", info: "role:master\r\n", invalid: true},
		{name: "negative", info: "connected_slaves:-1\r\n", invalid: true},
		{name: "duplicate", info: "connected_slaves:1\r\nconnected_slaves:2\r\n", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			mock.ExpectInfo("replication").SetVal(tc.info)
			got, err := (&service{redisClient: db}).GetAttachedReplicaCount(context.Background())
			require.Equal(t, tc.invalid, err != nil)
			require.Equal(t, tc.count, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestServiceWithoutConnectionFailsClosed(t *testing.T) {
	ctx := context.Background()
	for name, operation := range map[string]func(Service) error{
		"sentinel info":   func(s Service) error { _, err := s.GetInfoSentinel(ctx); return err },
		"master address":  func(s Service) error { _, _, _, err := s.SentinelGetMasterAddrByName(ctx, "upgrade"); return err },
		"sentinel config": func(s Service) error { return s.SentinelConfigSet(ctx, "resolve-hostnames", "yes") },
		"sentinel set":    func(s Service) error { return s.SentinelSet(ctx, "upgrade", "quorum", "2") },
		"sentinel reset":  func(s Service) error { return s.SentinelReset(ctx, "upgrade") },
		"sentinel monitor": func(s Service) error {
			return s.SentinelMonitor(ctx, &ConnectionInfo{Host: "127.0.0.1", Port: "6379"}, "upgrade", "2")
		},
		"role":     func(s Service) error { _, err := s.IsMaster(ctx); return err },
		"replicas": func(s Service) error { _, err := s.GetAttachedReplicaCount(ctx); return err },
		"keys":     func(s Service) error { _, err := s.GetKeyCount(ctx); return err },
		"cluster":  func(s Service) error { _, err := s.GetClusterInfo(ctx); return err },
	} {
		t.Run(name, func(t *testing.T) {
			for _, s := range []Service{NewService(), NewClient().Connect(nil)} {
				require.NotPanics(t, func() {
					require.ErrorContains(t, operation(s), "connection info is not set")
				})
			}
		})
	}
}

func TestCountKeyspaceKeys(t *testing.T) {
	tests := []struct {
		name string
		info string
		want int64
	}{
		{name: "empty instance", info: "# Keyspace\r\n", want: 0},
		{name: "single db", info: "# Keyspace\r\ndb0:keys=120,expires=3,avg_ttl=0\r\n", want: 120},
		{name: "multiple dbs", info: "# Keyspace\r\ndb0:keys=120,expires=3,avg_ttl=0\r\ndb3:keys=7,expires=0,avg_ttl=0,subexpiry=0\r\n", want: 127},
		{name: "zero keys", info: "# Keyspace\r\ndb0:keys=0,expires=0,avg_ttl=0\r\n", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := countKeyspaceKeys(tt.info)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCountKeyspaceKeysRejectsMalformedCount(t *testing.T) {
	_, err := countKeyspaceKeys("# Keyspace\r\ndb0:keys=many,expires=0,avg_ttl=0\r\n")
	assert.Error(t, err)
}
