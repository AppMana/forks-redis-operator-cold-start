package redisreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSentinelBootstrapPlaceholderIsNotAnElection(t *testing.T) {
	for _, placeholder := range []string{"0.0.0.0:6379", "[::]:6379"} {
		t.Run(placeholder, func(t *testing.T) {
			_, ok := majorityAddress([]string{placeholder, placeholder, placeholder}, 3)
			require.False(t, ok, "unconfigured Sentinels must not prevent guarded Redis bootstrap")
			_, ok = majorityAddress([]string{placeholder, "10.0.0.1:6379"}, 3)
			require.False(t, ok, "ignoring placeholders must not lower the configured majority threshold")
		})
	}
	addr, ok := majorityAddress([]string{"10.0.0.99:6379", "10.0.0.99:6379"}, 3)
	require.True(t, ok, "a real address remains authoritative even if its pod is absent")
	require.Equal(t, "10.0.0.99:6379", addr)
}
