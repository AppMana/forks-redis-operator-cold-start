package redis

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	redisservice "github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestMissingTLSSecretMustNotEnablePlaintext(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "redis-0"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}}
	config := &commonapi.TLSConfig{Secret: corev1.SecretVolumeSource{SecretName: "missing-cert"}}
	conn := createConnectionInfo(context.Background(), pod, "synthetic-only", config, k8sfake.NewSimpleClientset(), "test", "6379")
	require.NotNil(t, conn.TLSConfig, "nil TLS config makes go-redis send plaintext, including authentication")
	require.NotNil(t, conn.TLSConfig.VerifyConnection)
	require.Error(t, conn.TLSConfig.VerifyConnection(tls.ConnectionState{}))
}

func TestEmptyTLSSecretMustNotEnablePlaintext(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "redis-0"}, Status: corev1.PodStatus{PodIP: "10.0.0.1"}}
	client := k8sfake.NewSimpleClientset()
	config := &commonapi.TLSConfig{}
	conn := ConnectionInfoForPod(context.Background(), pod, "synthetic-only", config, client, "test", "6379")
	require.NotNil(t, conn.TLSConfig, "explicit TLS must not fall back to plaintext on a malformed Secret reference")
	require.NotNil(t, conn.TLSConfig.VerifyConnection)
	require.Error(t, conn.TLSConfig.VerifyConnection(tls.ConnectionState{}))
	direct := getRedisTLSConfig(context.Background(), client, "test", config)
	require.NotNil(t, direct)
	require.Error(t, direct.VerifyConnection(tls.ConnectionState{}))
	require.Empty(t, client.Actions(), "invalid reference must not attempt an empty-name Secret lookup")
}

func TestAbsentTLSStillUsesPlaintext(t *testing.T) {
	pod := corev1.Pod{Status: corev1.PodStatus{PodIP: "10.0.0.1"}}
	client := k8sfake.NewSimpleClientset()
	conn := ConnectionInfoForPod(context.Background(), pod, "", nil, client, "test", "6379")
	require.Nil(t, conn.TLSConfig)
	require.Equal(t, pod.Status.PodIP, conn.Host)
	require.Nil(t, getRedisTLSConfig(context.Background(), client, "test", nil))
	require.Empty(t, client.Actions())
}

func TestMalformedTLSDoesNotSendPlaintextRedisCommands(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	firstByte := make(chan byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			firstByte <- 0
			return
		}
		defer conn.Close()
		defer listener.Close()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var b [1]byte
		_, err = conn.Read(b[:])
		if err != nil {
			firstByte <- 0
			return
		}
		firstByte <- b[0]
	}()
	conn := ConnectionInfoForPod(ctx, corev1.Pod{}, "synthetic-only", &commonapi.TLSConfig{}, k8sfake.NewSimpleClientset(), "test", "6379")
	conn.Host, conn.Port, err = net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	_, err = redisservice.NewClient().Connect(conn).IsMaster(ctx)
	require.Error(t, err)
	select {
	case b := <-firstByte:
		require.Equal(t, byte(22), b, "connection must start with a TLS handshake, never a plaintext RESP command")
	case <-ctx.Done():
		t.Fatal("did not observe connection bytes within deadline")
	}
}
