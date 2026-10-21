package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestSentinelInfoRejectsUnknownTopology(t *testing.T) {
	valid := "master0:name=group,status=ok,address=127.0.0.1:6379,slaves=0,sentinels=3"
	for _, raw := range []string{
		"", valid,
		"sentinel_masters:1\n",
		"sentinel_masters:2\n" + valid,
		"sentinel_masters:1\nsentinel_masters:1\n" + valid,
		"sentinel_masters:1\n" + strings.Replace(valid, "slaves=0", "slaves=invalid", 1),
		"sentinel_masters:1\n" + strings.Replace(valid, "slaves=0,", "", 1),
		"sentinel_masters:1\n" + strings.Replace(valid, "sentinels=3", "sentinels=-1", 1),
		"sentinel_masters:1\n" + valid + ",slaves=7",
		"sentinel_masters:2\n" + valid + "\n" + valid,
		"sentinel_masters:2\n" + valid + "\n" + strings.Replace(valid, "master0", "master1", 1),
	} {
		db, mock := redismock.NewClientMock()
		mock.ExpectInfo("sentinel").SetVal(raw)
		info, err := (&service{redisClient: db}).GetInfoSentinel(context.Background())
		require.Error(t, err, "malformed INFO must not become a successful topology")
		require.Nil(t, info)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestSentinelInfoAcceptsCompleteTopology(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		for _, raw := range []string{
			"# Sentinel\nsentinel_masters:0\n",
			"# Sentinel\nsentinel_masters:1\nmaster0:name=group,status=ok,address=127.0.0.1:6379,slaves=0,sentinels=3\n",
		} {
			db, mock := redismock.NewClientMock()
			mock.ExpectInfo("sentinel").SetVal(strings.ReplaceAll(raw, "\n", newline))
			info, err := (&service{redisClient: db}).GetInfoSentinel(context.Background())
			require.NoError(t, err)
			if strings.Contains(raw, "master0") {
				require.Equal(t, []SentinelMasterInfo{{Name: "group", Status: "ok", Address: "127.0.0.1:6379", Slaves: 0, Sentinels: 3}}, info.Masters)
			} else {
				require.Empty(t, info.Masters)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		}
	}
}
