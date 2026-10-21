package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestSentinelMonitorPreservesExistingGroupWhenHostnamesDisabled(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal([]interface{}{"ip", "10.0.0.1", "port", "6379"})
	mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetVal([]interface{}{"resolve-hostnames", "no"})
	// No REMOVE is expected: rejecting MONITOR after REMOVE loses the group.
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.redis-headless.ns.svc.cluster.local", Port: "6379"}, "upgrade", "2")
	require.ErrorContains(t, err, "resolve-hostnames")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorPreservesGroupWhenResolverCannotBeChecked(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal([]interface{}{"ip", "10.0.0.1", "port", "6379"})
	mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetErr(errors.New("resolver setting unavailable"))
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2")
	require.ErrorContains(t, err, "resolver setting unavailable")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorDoesNotTouchAlreadyCorrectHostname(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal([]interface{}{"ip", "redis-0.example", "port", "6379"})
	require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2"))
	require.NoError(t, mock.ExpectationsWereMet())
}
