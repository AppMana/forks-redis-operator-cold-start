package k8sutils

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/require"
)

func TestFollowerRetirementPreservesRetainedPods(t *testing.T) {
	require.Equal(t, []string{"old"}, selectRetiringFollowers([]string{"retained", "old"}, map[string]bool{"old": true, "other-shard": true}))
}

func TestFollowerRetirementValidatesBeforeRemoval(t *testing.T) {
	for _, scenario := range []string{"success", "observation fails", "owns slots", "removal fails"} {
		t.Run(scenario, func(t *testing.T) {
			var calls []string
			err := retireEmptyFollowers([]string{"a", "b"}, func(id string) (int, error) {
				calls = append(calls, "observe:"+id)
				if id == "b" && scenario == "observation fails" {
					return 0, errors.New("failed")
				}
				if id == "b" && scenario == "owns slots" {
					return 1, nil
				}
				return 0, nil
			}, func(id string) error {
				calls = append(calls, "remove:"+id)
				if scenario == "removal fails" {
					return errors.New("failed")
				}
				return nil
			})
			switch scenario {
			case "success":
				require.NoError(t, err)
				require.Equal(t, []string{"observe:a", "observe:b", "remove:a", "remove:b"}, calls)
			case "removal fails":
				require.Error(t, err)
				require.Equal(t, []string{"observe:a", "observe:b", "remove:a"}, calls)
			default:
				require.Error(t, err)
				require.Equal(t, []string{"observe:a", "observe:b"}, calls)
			}
		})
	}
}

func TestAttachedFollowerInventoryRejectsMalformedRows(t *testing.T) {
	for _, rows := range [][]string{
		{""},
		{"id"},
		{"id addr master - 0 0 1 connected"},
		{"id addr slave wrong-master 0 0 1 connected"},
		{"id addr slave source 0 0 1 connected", "id addr slave source 0 0 1 connected"},
	} {
		c, m := redismock.NewClientMock()
		m.ExpectClusterSlaves("source").SetVal(rows)
		ids, err := getAttachedFollowerNodeIDs(context.Background(), c, "source")
		require.Error(t, err)
		require.Nil(t, ids)
		require.NoError(t, m.ExpectationsWereMet())
		_ = c.Close()
	}
}
