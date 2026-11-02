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
	peers  []redis.SentinelPeerInfo
	resets int
}

func (s *membershipObservationService) GetInfoSentinel(context.Context) (*redis.InfoSentinelResult, error) {
	return s.info, nil
}

func (s *membershipObservationService) SentinelPeers(context.Context, string) ([]redis.SentinelPeerInfo, error) {
	return s.peers, nil
}

func (s *membershipObservationService) SentinelReset(context.Context, string) error {
	s.resets++
	return nil
}

func TestEmbeddedSentinelResetsStaleMembershipWhenMasterIsHealthy(t *testing.T) {
	for _, observed := range []redis.SentinelMasterInfo{
		{Name: masterGroupName, Status: "ok", Slaves: 2, Sentinels: 7},
		{Name: masterGroupName, Status: "ok", Slaves: 3, Sentinels: 3},
	} {
		service := &membershipObservationService{info: &redis.InfoSentinelResult{Masters: []redis.SentinelMasterInfo{observed}}}
		err := (&Reconciler{}).sentinelResetIfNeed(context.Background(), newSentinelReplicationInstanceForTest(), service)
		require.NoError(t, err)
		require.Equal(t, 1, service.resets)
	}
}

func TestEmbeddedSentinelSkipsResetWhenMasterIsDown(t *testing.T) {
	service := &membershipObservationService{
		info: &redis.InfoSentinelResult{Masters: []redis.SentinelMasterInfo{
			{Name: masterGroupName, Status: "odown", Slaves: 2, Sentinels: 7},
		}},
	}
	err := (&Reconciler{}).sentinelResetIfNeed(context.Background(), newSentinelReplicationInstanceForTest(), service)
	require.NoError(t, err)
	require.Zero(t, service.resets)
}
