package controllerutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
)

func TestSuccessfulSentinelRepairUsesBoundedObservationRetry(t *testing.T) {
	pending := fmt.Errorf("repair batch: %w", redis.ErrSentinelMonitorPending)
	result, err := RequeueE(context.Background(), pending, "")
	require.NoError(t, err, "successful repair must reset accumulated error backoff")
	require.Equal(t, 2*time.Second, result.RequeueAfter)
	failure := errors.New("monitor mutation failed")
	_, err = RequeueE(context.Background(), failure, "")
	require.ErrorIs(t, err, failure, "real failures must retain normal retry semantics")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = RequeueE(ctx, pending, "")
	require.Error(t, err, "cancellation is not successful reconciliation")
}
