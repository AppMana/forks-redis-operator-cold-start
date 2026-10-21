package redisreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestBootstrapPlaceholder(t *testing.T) {
	primary := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "redis-0", Namespace: "test"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}}
	real := bootstrapMonitor{host: "10.0.0.1", port: "6379", monitored: true, healthy: true}
	blank := bootstrapMonitor{host: "0.0.0.0", port: "6379", monitored: true}
	for _, tt := range []struct {
		name   string
		states []bootstrapMonitor
		want   int
	}{
		{"partial bootstrap", []bootstrapMonitor{real, blank, blank}, 2},
		{"resume after restart", []bootstrapMonitor{real, real, blank}, 2},
		{"all initialized", []bootstrapMonitor{real, real, real}, -1},
		{"all placeholders", []bootstrapMonitor{blank, blank, blank}, -1},
		{"missing member", []bootstrapMonitor{real, blank}, -1},
		{"absent monitor", []bootstrapMonitor{real, blank, {}}, -1},
		{"conflicting monitor", []bootstrapMonitor{real, blank, {host: "10.0.0.2", port: "6379", monitored: true, healthy: true}}, -1},
		{"unhealthy real monitor", []bootstrapMonitor{{host: real.host, port: real.port, monitored: true}, blank, blank}, -1},
		{"wrong placeholder port", []bootstrapMonitor{real, blank, {host: "0.0.0.0", port: "6380", monitored: true}}, -1},
		{"wrong primary port", []bootstrapMonitor{{host: real.host, port: "26379", monitored: true, healthy: true}, blank, blank}, -1},
		{"unknown address", []bootstrapMonitor{real, blank, {host: "unknown", port: "6379", monitored: true, healthy: true}}, -1},
		{"IPv6 placeholder", []bootstrapMonitor{real, blank, {host: "::", port: "6379", monitored: true}}, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := bootstrapPlaceholder(tt.states, 3, primary); got != tt.want {
				t.Fatalf("selected %d, want %d", got, tt.want)
			}
		})
	}
}
