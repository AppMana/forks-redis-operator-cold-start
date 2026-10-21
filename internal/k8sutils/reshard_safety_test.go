package k8sutils

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redismock/v9"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestReshardBeforeRemoval(t *testing.T) {
	failure := errors.New("injected failure")
	for _, tt := range []struct {
		name                                             string
		initial, remaining                               int
		initialErr, remainingErr, transferErr, removeErr error
		wantTransfer, wantRemove, wantErr                bool
	}{
		{name: "initial observation failed", initialErr: failure, wantErr: true},
		{name: "invalid count", initial: -1, wantErr: true},
		{name: "transfer failed", initial: 42, transferErr: failure, wantTransfer: true, wantErr: true},
		{name: "post-transfer observation failed", initial: 42, remainingErr: failure, wantTransfer: true, wantErr: true},
		{name: "source still owns slots", initial: 42, remaining: 1, wantTransfer: true, wantErr: true},
		{name: "removal failed", initial: 42, removeErr: failure, wantTransfer: true, wantRemove: true, wantErr: true},
		{name: "successful transfer", initial: 42, wantTransfer: true, wantRemove: true},
		{name: "retry after completed transfer", wantRemove: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			transferred, removed := false, false
			err := reshardBeforeRemoval(func() (int, error) {
				reads++
				if reads == 1 {
					return tt.initial, tt.initialErr
				}
				return tt.remaining, tt.remainingErr
			}, func(slots int) error {
				require.Equal(t, tt.initial, slots)
				transferred = true
				return tt.transferErr
			}, func() error {
				removed = true
				return tt.removeErr
			})
			require.Equal(t, tt.wantTransfer, transferred)
			require.Equal(t, tt.wantRemove, removed)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOwnedRedisClusterSlots(t *testing.T) {
	for _, tt := range []struct {
		name    string
		slots   []redis.ClusterSlot
		readErr error
		want    int
		wantErr bool
	}{
		{name: "primary owns slots", slots: []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{ID: "source"}}}}, want: 16384},
		{name: "replica does not own slots", slots: []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{ID: "target"}, {ID: "source"}}}}},
		{name: "read failed", readErr: errors.New("unavailable"), wantErr: true},
		{name: "empty", wantErr: true},
		{name: "partial", slots: []redis.ClusterSlot{{Start: 0, End: 100, Nodes: []redis.ClusterNode{{ID: "target"}}}}, wantErr: true},
		{name: "owner missing", slots: []redis.ClusterSlot{{Start: 0, End: 16383}}, wantErr: true},
		{name: "overlap", slots: []redis.ClusterSlot{
			{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{ID: "target"}}},
			{Start: 0, End: 1, Nodes: []redis.ClusterNode{{ID: "source"}}},
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := redismock.NewClientMock()
			t.Cleanup(func() { _ = client.Close() })
			if tt.readErr != nil {
				mock.ExpectClusterSlots().SetErr(tt.readErr)
			} else {
				mock.ExpectClusterSlots().SetVal(tt.slots)
			}
			got, err := ownedRedisClusterSlots(context.Background(), client, "source")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
