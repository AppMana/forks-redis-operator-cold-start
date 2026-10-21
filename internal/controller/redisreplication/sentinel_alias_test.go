package redisreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSentinelVotesUsePodIdentityNotAddressSpelling(t *testing.T) {
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "redis-0", Namespace: "team-a"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}}}
	fqdn := "redis-0.redis-headless.team-a.svc.cluster.local:6379"
	ip := "10.0.0.1:6379"
	require.Equal(t, ip, canonicalSentinelAddress(pods, fqdn))
	require.Equal(t, ip, canonicalSentinelAddress(pods, "redis-0.redis-headless.team-a.svc.cluster.local.:6379"))
	addr, ok := majorityAddress([]string{canonicalSentinelAddress(pods, fqdn), canonicalSentinelAddress(pods, ip)}, 3)
	require.True(t, ok, "two representations of one live primary form a majority")
	require.Equal(t, ip, addr)
	for _, other := range []string{
		"redis-0.redis-headless.team-b.svc.cluster.local:6379",
		"redis-0.redis-headless.team-a.svc.cluster.local:26379",
		"10.0.0.99:6379",
	} {
		require.Equal(t, other, canonicalSentinelAddress(pods, other), "unknown addresses must remain distinct")
		_, ok := majorityAddress([]string{ip, canonicalSentinelAddress(pods, other)}, 3)
		require.False(t, ok)
	}
}

func TestSentinelVotesIncludeObservedSurplusMembers(t *testing.T) {
	oldPrimary, newPrimary := "10.0.0.1:6379", "10.0.0.2:6379"
	for _, answers := range [][]string{
		{oldPrimary, oldPrimary, oldPrimary, newPrimary, newPrimary, newPrimary, newPrimary},
		{newPrimary, newPrimary, newPrimary, newPrimary, oldPrimary, oldPrimary, oldPrimary},
	} {
		address, ok := majorityAddress(answers, 3)
		require.True(t, ok)
		require.Equal(t, newPrimary, address, "surplus observed voters must not permit competing majorities")
	}
	_, ok := majorityAddress([]string{oldPrimary, oldPrimary, newPrimary, newPrimary}, 3)
	require.False(t, ok, "a split surplus inventory has no majority")
	_, ok = majorityAddress([]string{oldPrimary, oldPrimary, oldPrimary}, 7)
	require.False(t, ok, "unreachable members must remain in the denominator")
}
