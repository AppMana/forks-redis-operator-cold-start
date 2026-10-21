package k8sutils

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestReplicationReconfigurationReconnectsNormalClients(t *testing.T) {
	for _, info := range []string{
		"role:master\r\n",
		"role:slave\r\nmaster_host:10.0.0.9\r\nmaster_port:6379\r\n",
	} {
		t.Run(info, func(t *testing.T) {
			client, mock := redismock.NewClientMock()
			mock.ExpectInfo("Replication").SetVal(info)
			mock.ExpectTxPipeline()
			mock.ExpectSlaveOf("10.0.0.2", "6379").SetVal("OK")
			mock.ExpectClientKillByFilter("TYPE", "normal", "SKIPME", "yes").SetVal(4)
			mock.ExpectTxPipelineExec()
			require.NoError(t, reconfigureReplicationClient(context.Background(), client, "10.0.0.2"))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReplicationReconfigurationDoesNotDisconnectUnchangedReplica(t *testing.T) {
	client, mock := redismock.NewClientMock()
	mock.ExpectInfo("Replication").SetVal("role:slave\r\nmaster_host:10.0.0.2\r\nmaster_port:6379\r\n")
	require.NoError(t, reconfigureReplicationClient(context.Background(), client, "10.0.0.2"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplicationReconfigurationFailsClosedOnUnknownRole(t *testing.T) {
	client, mock := redismock.NewClientMock()
	mock.ExpectInfo("Replication").SetErr(errors.New("unreachable"))
	require.Error(t, reconfigureReplicationClient(context.Background(), client, "10.0.0.2"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplicationReconfigurationRejectsAmbiguousObservations(t *testing.T) {
	for _, info := range []string{
		"", "role:master\r\nrole:slave\r\n", "role:slave\r\n",
		"role:slave\r\nmaster_host:10.0.0.2\r\nmaster_host:10.0.0.3\r\nmaster_port:6379\r\n",
	} {
		client, mock := redismock.NewClientMock()
		mock.ExpectInfo("Replication").SetVal(info)
		require.Error(t, reconfigureReplicationClient(context.Background(), client, "10.0.0.2"))
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestReplicationReconfigurationReportsTransactionFailure(t *testing.T) {
	client, mock := redismock.NewClientMock()
	mock.ExpectInfo("Replication").SetVal("role:master\r\n")
	mock.ExpectTxPipeline()
	mock.ExpectSlaveOf("10.0.0.2", "6379").SetVal("OK")
	mock.ExpectClientKillByFilter("TYPE", "normal", "SKIPME", "yes").SetVal(0)
	mock.ExpectTxPipelineExec().SetErr(errors.New("EXECABORT"))
	require.ErrorContains(t, reconfigureReplicationClient(context.Background(), client, "10.0.0.2"), "EXECABORT")
	require.NoError(t, mock.ExpectationsWereMet())
}
