package k8sutils

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetireFollowerOrdinals(t *testing.T) {
	for _, tc := range []struct {
		name           string
		ids            []string
		alreadyRetired string
		viewError      string
		slotError      string
		slotOwner      string
		removeError    string
		wantRemoved    []string
		wantErr        bool
	}{
		{name: "follower-only or reparented copies", ids: []string{"keep", "old1", "old2"}, wantRemoved: []string{"old2", "old1"}},
		{name: "resume partial retirement", ids: []string{"keep", "old1", "old2"}, alreadyRetired: "old2", wantRemoved: []string{"old1"}},
		{name: "unknown identity", ids: []string{"keep", "", "old2"}, wantErr: true},
		{name: "retained identity", ids: []string{"keep", "keep", "old2"}, wantErr: true},
		{name: "duplicate identity", ids: []string{"keep", "old2", "old2"}, wantErr: true},
		{name: "incomplete view", ids: []string{"keep", "old1", "old2"}, viewError: "old1", wantErr: true},
		{name: "slot lookup fails", ids: []string{"keep", "old1", "old2"}, slotError: "old1", wantErr: true},
		{name: "promoted follower owns slots", ids: []string{"keep", "old1", "old2"}, slotOwner: "old1", wantErr: true},
		{name: "uncertain removal stops subsequent nodes", ids: []string{"keep", "old1", "old2"}, removeError: "old2", wantRemoved: []string{"old2"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var removed, observations []string
			err := retireFollowerOrdinals(3, 1, map[string]bool{"keep": true, "leader": true}, func(i int32) string {
				require.GreaterOrEqual(t, i, int32(1), "retained ordinal must not be retired")
				return tc.ids[i]
			}, func(id string) (bool, error) {
				if id == tc.viewError {
					return false, errors.New("observation failed")
				}
				return id == tc.alreadyRetired, nil
			}, func(id string) (int, error) {
				require.Empty(t, removed, "all candidate slots must be observed before any removal")
				observations = append(observations, id)
				if id == tc.slotError {
					return 0, errors.New("slots unavailable")
				}
				if id == tc.slotOwner {
					return 1, nil
				}
				return 0, nil
			}, func(id string) error {
				require.Contains(t, observations, id)
				removed = append(removed, id)
				if id == tc.removeError {
					return fmt.Errorf("uncertain removal")
				}
				return nil
			})
			require.Equal(t, tc.wantRemoved, removed)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
