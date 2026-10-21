package k8sutils

import (
	"context"
	"errors"
	"testing"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/go-redis/redismock/v9"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestClusterRetirementProgress(t *testing.T) {
	cr := &rcvb2.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "test"}, Spec: rcvb2.RedisClusterSpec{ClusterSize: ptr.To(int32(1))}}
	retained := "a addr master - 0 0 1 connected 0-16383\nb addr slave a 0 0 1 connected\n"
	withSource := retained + "old addr master - 0 0 1 connected\n"
	for _, tc := range []struct {
		name        string
		views       []string
		unhealthy   bool
		wantRetired bool
		wantErr     bool
	}{
		{name: "already removed shard", views: []string{retained, retained}, wantRetired: true},
		{name: "remaining shard still needs retirement", views: []string{withSource, withSource}},
		{name: "partial forget must wait", views: []string{retained, withSource}, wantErr: true},
		{name: "reverse disagreement must wait", views: []string{withSource, retained}, wantErr: true},
		{name: "unavailable follower view", views: []string{retained, "error"}, wantErr: true},
		{name: "unavailable leader view", views: []string{"error"}, wantErr: true},
		{name: "empty membership", views: []string{""}, wantErr: true},
		{name: "malformed membership", views: []string{"old addr"}, wantErr: true},
		{name: "duplicate membership", views: []string{retained + retained}, wantErr: true},
		{name: "absent in unhealthy view", views: []string{retained}, unhealthy: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := 0
			retired, err := clusterLeaderRetired(context.Background(), cr, "old", func(pod string) *redis.Client {
				require.Less(t, n, len(tc.views))
				view := tc.views[n]
				n++
				c, m := redismock.NewClientMock()
				t.Cleanup(func() { require.NoError(t, m.ExpectationsWereMet()) })
				if view == "error" {
					m.ExpectClusterNodes().SetErr(errors.New("unreachable"))
				} else {
					m.ExpectClusterNodes().SetVal(view)
					if view == retained {
						if tc.unhealthy {
							m.ExpectClusterInfo().SetVal("cluster_state:fail\r\n")
						} else {
							m.ExpectClusterInfo().SetVal(servingClusterInfo)
						}
					}
				}
				return c
			})
			require.Equal(t, tc.wantRetired, retired)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.views), n)
		})
	}
	_, err := clusterLeaderRetired(context.Background(), cr, "", func(string) *redis.Client {
		t.Fatal("unknown source must not issue commands")
		return nil
	})
	require.Error(t, err)
}
