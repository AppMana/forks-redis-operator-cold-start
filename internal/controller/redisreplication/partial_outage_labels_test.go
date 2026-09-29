package redisreplication

import (
	"context"
	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/stretchr/testify/require"
	"testing"
)

type recordingRoleHealer struct {
	redishealer.Healer
	calls int
}

func (h *recordingRoleHealer) UpdateRedisRoleLabel(context.Context, string, map[string]string, *commonapi.ExistingPasswordSecret, *commonapi.TLSConfig) error {
	h.calls++
	return nil
}

func TestPublishObservedRoleLabels(t *testing.T) {
	for _, tc := range []struct {
		name    string
		masters []string
		calls   int
	}{
		{"elected primary despite absent replica", []string{"redis-0"}, 1},
		{"empty replacement must not join writable service", []string{"redis-0", "redis-1"}, 0},
		{"unknown primary must not publish roles", nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &recordingRoleHealer{}
			r := &Reconciler{Healer: h}
			require.NoError(t, r.publishObservedRoleLabels(context.Background(), &rrvb2.RedisReplication{}, tc.masters))
			require.Equal(t, tc.calls, h.calls)
		})
	}
}
