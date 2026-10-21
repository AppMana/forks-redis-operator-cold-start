package redis

import (
	"context"
	"errors"
	"net"
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
	svc := &service{redisClient: db, lookupHost: func(context.Context, string) ([]string, error) { return []string{"10.0.0.2"}, nil }}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379"})
	mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetVal(map[string]string{"resolve-hostnames": "yes"})
	mock.ExpectDo("SENTINEL", "REMOVE", "upgrade").SetVal(true)
	mock.ExpectDo("SENTINEL", "MONITOR", "upgrade", "redis-0.example", "6379", "2").SetVal(true)
	mock.ExpectDo("SENTINEL", "SET", "upgrade", "auth-pass", "").SetVal(true)
	require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorPreservesGroupWhenTargetDNSFails(t *testing.T) {
	for _, lookupErr := range []error{&net.DNSError{Err: "no such host", IsNotFound: true}, context.DeadlineExceeded, nil} {
		db, mock := redismock.NewClientMock()
		svc := &service{redisClient: db, lookupHost: func(context.Context, string) ([]string, error) { return nil, lookupErr }}
		mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379"})
		mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames").SetVal(map[string]string{"resolve-hostnames": "yes"})
		err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "missing.invalid", Port: "6379"}, "upgrade", "2")
		require.ErrorContains(t, err, "resolve replacement Sentinel master")
		require.NoError(t, mock.ExpectationsWereMet())
	}
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

func TestSentinelMonitorPreservesAlreadyCorrectHostname(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "redis-0.example", "port": "6379", "quorum": "2"})
	mock.ExpectDo("SENTINEL", "SET", "upgrade", "auth-pass", "").SetVal(true)
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
	mock.ExpectDo("SENTINEL", "SET", "upgrade", "auth-pass", "").SetVal(true)
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

func TestSentinelMonitorReconcilesCredentialsWithoutResettingGroup(t *testing.T) {
	for _, password := range []string{"synthetic-rotated-password", ""} {
		t.Run(map[bool]string{true: "remove", false: "rotate"}[password == ""], func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			svc := &service{redisClient: db}
			mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379", "quorum": "2"})
			mock.ExpectDo("SENTINEL", "SET", "upgrade", "auth-pass", password).SetVal(true)
			require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.1", Port: "6379", Password: password}, "upgrade", "2"))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSentinelMonitorReportsCredentialUpdateFailure(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379", "quorum": "2"})
	updateErr := errors.New("credential update unavailable")
	mock.ExpectDo("SENTINEL", "SET", "upgrade", "auth-pass", "synthetic-password").SetErr(updateErr)
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.1", Port: "6379", Password: "synthetic-password"}, "upgrade", "2")
	require.ErrorIs(t, err, updateErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorReconcilesQuorumWithoutReset(t *testing.T) {
	for _, current := range []string{"1", "3", ""} {
		t.Run("current="+current, func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			svc := &service{redisClient: db}
			mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379", "quorum": current})
			mock.ExpectDo("SENTINEL", "SET", "upgrade", "quorum", "2").SetVal("OK")
			mock.ExpectDo("SENTINEL", "SET", "upgrade", "auth-pass", "").SetVal(true)
			require.NoError(t, svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.1", Port: "6379"}, "upgrade", "2"))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSentinelMonitorReportsQuorumUpdateFailure(t *testing.T) {
	db, mock := redismock.NewClientMock()
	svc := &service{redisClient: db}
	mock.ExpectDo("SENTINEL", "MASTER", "upgrade").SetVal(map[string]string{"ip": "10.0.0.1", "port": "6379", "quorum": "3"})
	updateErr := errors.New("quorum update unavailable")
	mock.ExpectDo("SENTINEL", "SET", "upgrade", "quorum", "2").SetErr(updateErr)
	err := svc.SentinelMonitor(context.Background(), &ConnectionInfo{Host: "10.0.0.1", Port: "6379"}, "upgrade", "2")
	require.ErrorIs(t, err, updateErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSentinelMonitorRejectsInvalidReplacementBeforeRedisAccess(t *testing.T) {
	for _, tc := range []struct {
		name   string
		master *ConnectionInfo
		quorum string
		want   string
	}{
		{name: "missing master", quorum: "2", want: "master address"},
		{name: "empty host", master: &ConnectionInfo{Port: "6379"}, quorum: "2", want: "master address"},
		{name: "zero port", master: &ConnectionInfo{Host: "10.0.0.2", Port: "0"}, quorum: "2", want: "master port"},
		{name: "overflow port", master: &ConnectionInfo{Host: "10.0.0.2", Port: "65536"}, quorum: "2", want: "master port"},
		{name: "invalid port", master: &ConnectionInfo{Host: "10.0.0.2", Port: "invalid"}, quorum: "2", want: "master port"},
		{name: "zero quorum", master: &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, quorum: "0", want: "quorum"},
		{name: "negative quorum", master: &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, quorum: "-1", want: "quorum"},
		{name: "invalid quorum", master: &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, quorum: "invalid", want: "quorum"},
		{name: "overflow quorum", master: &ConnectionInfo{Host: "10.0.0.2", Port: "6379"}, quorum: "2147483648", want: "quorum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			svc := &service{redisClient: db}
			// No commands, particularly REMOVE, may precede validation.
			require.NotPanics(t, func() {
				require.ErrorContains(t, svc.SentinelMonitor(context.Background(), tc.master, "upgrade", tc.quorum), tc.want)
			})
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
