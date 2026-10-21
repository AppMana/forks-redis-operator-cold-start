package redisreplication

import (
	"context"
	"testing"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
)

type membershipObservationService struct {
	redis.Service
	info   *redis.InfoSentinelResult
	resets int
}

func (s *membershipObservationService) GetInfoSentinel(context.Context) (*redis.InfoSentinelResult, error) {
	return s.info, nil
}

func (s *membershipObservationService) SentinelReset(context.Context, string) error {
	s.resets++
	return nil
}

func TestEmbeddedSentinelPreservesSurplusMembership(t *testing.T) {
	for _, observed := range []redis.SentinelMasterInfo{
		{Name: masterGroupName, Slaves: 2, Sentinels: 7},
		{Name: masterGroupName, Slaves: 3, Sentinels: 3},
	} {
		service := &membershipObservationService{info: &redis.InfoSentinelResult{Masters: []redis.SentinelMasterInfo{observed}}}
		err := (&Reconciler{}).sentinelResetIfNeed(context.Background(), newSentinelReplicationInstanceForTest(), service)
		require.NoError(t, err)
		require.Zero(t, service.resets, "counts cannot authorize membership removal or clearing election state")
	}
}
