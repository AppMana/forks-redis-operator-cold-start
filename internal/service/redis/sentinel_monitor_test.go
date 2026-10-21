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
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379"})
	mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetVal(map[string]string{"resolve-hostnames": "no"})
	// No REMOVE is expected: rejecting MONITOR after REMOVE loses the group.
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.redis-headless.ns.svc.cluster.local", Port: "6379"}, "upgrade", "2")
	require.ErrorContains(t, err, "resolve-hostnames")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorAcceptsEnabledResolverMap(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379"})
	mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetVal(map[string]string{"resolve-hostnames": "yes"})
	mock.ExpectDo("SENTINEL", "REMOVE", "upgrade").SetVal(true)
	mock.ExpectDo("SENTINEL", "MONITOR", "upgrade", "redis-0.example", "6379", "2").SetVal(true)
	require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorPreservesGroupWhenResolverCannotBeChecked(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379"})
	mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetErr(errors.New("resolver setting unavailable"))
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2")
	require.ErrorContains(t, err, "resolver setting unavailable")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorDoesNotTouchAlreadyCorrectHostname(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "redis-0.example", "port": "6379"})
	require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2"))
	require.NoError(t, mock.ExpectationsWereMet())
}

type sentinelTestError string

func (e sentinelTestError) Error() string { return string(e) }
func (sentinelTestError) RedisError()     {}

func TestSentinelMonitorDoesNotMutateUnknownState(t *testing.T) {
	for _, lookupErr := range []error{context.DeadlineExceeded, sentinelTestError("NOAUTH Authentication required"), errors.New("connection reset")} {
		t.Run(lookupErr.Error(), func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			svc := &service{redisClient: db}
			mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetErr(lookupErr)
			err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, "upgrade", "2")
			require.ErrorIs(t, err, lookupErr)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSentinelMonitorCreatesOnlyConfirmedMissingGroup(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetErr(sentinelTestError("ERR No such master with that name"))
	mock.ExpectDo("SENTINEL", "MONITOR", "upgrade", "10.0.0.2", "6379", "2").SetVal(true)
	require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, "upgrade", "2"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorPreservesMalformedExistingGroup(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1"})
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, "upgrade", "2")
	require.ErrorContains(t, err, "incomplete Sentinel master metadata")
	require.NoError(t, mock.ExpectationsWereMet())
}
