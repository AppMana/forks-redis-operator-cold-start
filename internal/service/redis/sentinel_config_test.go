package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestSentinelConfigReconciliation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		current  string
		readErr  error
		writeErr error
	}{
		{name: "enable", current: "no"},
		{name: "already enabled", current: "yes"},
		{name: "unknown configuration"},
		{name: "read failure", readErr: errors.New("unavailable")},
		{name: "write failure", current: "no", writeErr: errors.New("denied")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := redismock.NewClientMock()
			svc := &service{redisClient: db}
			get := mock.ExpectDo("SENTINEL", "CONFIG", "GET", "resolve-hostnames")
			if tc.readErr != nil {
				get.SetErr(tc.readErr)
			} else if tc.current == "" {
				get.SetVal(map[string]string{})
			} else {
				get.SetVal(map[string]string{"resolve-hostnames": tc.current})
			}
			if tc.current == "no" && tc.readErr == nil {
				set := mock.ExpectDo("SENTINEL", "CONFIG", "SET", "resolve-hostnames", "yes")
				if tc.writeErr != nil {
					set.SetErr(tc.writeErr)
				} else {
					set.SetVal("OK")
				}
			}
			err := svc.SentinelConfigSet(context.Background(), "resolve-hostnames", "yes")
			if tc.readErr != nil || tc.writeErr != nil || tc.current == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
