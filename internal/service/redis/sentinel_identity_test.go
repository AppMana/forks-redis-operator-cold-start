package redis

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestSentinelIdentityRequiresExactHexIdentity(t *testing.T) {
	for _, id := range []string{strings.Repeat("a", 40), strings.Repeat("B", 40), "", strings.Repeat("g", 40), strings.Repeat("a", 39)} {
		client, mock := redismock.NewClientMock()
		mock.ExpectDo("SENTINEL", "MYID").SetVal(id)
		got, err := (&service{redisClient: client}).SentinelMyID(context.Background())
		if len(id) == 40 && !strings.Contains(id, "g") {
			require.NoError(t, err)
			require.Equal(t, strings.ToLower(id), got)
		} else {
			require.Error(t, err)
			require.Empty(t, got)
		}
		require.NoError(t, mock.ExpectationsWereMet())
	}
	client, mock := redismock.NewClientMock()
	failure := errors.New("identity unavailable")
	mock.ExpectDo("SENTINEL", "MYID").SetErr(failure)
	_, err := (&service{redisClient: client}).SentinelMyID(context.Background())
	require.ErrorIs(t, err, failure)
	_, err = (&service{}).SentinelMyID(context.Background())
	require.ErrorIs(t, err, errConnectionInfoNotSet)
}
