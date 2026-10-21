package rediscluster

import (
	"errors"
	"testing"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestReadyClusterContinuesConfigurationConvergence(t *testing.T) {
	instance := &rcvb2.RedisCluster{
		Spec:   rcvb2.RedisClusterSpec{ClusterSize: ptr.To(int32(3))},
		Status: rcvb2.RedisClusterStatus{State: rcvb2.RedisClusterReady, ReadyLeaderReplicas: 3, ReadyFollowerReplicas: 3},
	}
	for _, failure := range []error{nil, errors.New("pod unavailable"), nil} {
		calls := 0
		ready, err := configureHealthyCluster(instance, func() bool { return true }, func() error {
			calls++
			return failure
		})
		require.Equal(t, 1, calls, "Ready must not suppress config edits or retry")
		require.Equal(t, failure == nil, ready)
		require.ErrorIs(t, err, failure)
		require.Equal(t, rcvb2.RedisClusterReady, instance.Status.State)
	}
}

func TestClusterConfigurationWaitsForHealth(t *testing.T) {
	instance := &rcvb2.RedisCluster{
		Spec:   rcvb2.RedisClusterSpec{ClusterSize: ptr.To(int32(3))},
		Status: rcvb2.RedisClusterStatus{ReadyLeaderReplicas: 3, ReadyFollowerReplicas: 2},
	}
	apply := func() error { t.Fatal("must not configure an unready cluster"); return nil }
	ready, err := configureHealthyCluster(instance, func() bool { t.Fatal("pods missing"); return true }, apply)
	require.NoError(t, err)
	require.False(t, ready)
	instance.Status.ReadyFollowerReplicas = 3
	ready, err = configureHealthyCluster(instance, func() bool { return false }, apply)
	require.NoError(t, err)
	require.False(t, ready)
}
