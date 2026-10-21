package cryptutil

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnavailableTLSRejectsEvenTrustedServer(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	trusted := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	address := strings.TrimPrefix(server.URL, "https://")
	dialer := &tls.Dialer{Config: trusted}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	require.NoError(t, err, "baseline server must be trusted")
	require.NoError(t, conn.Close())

	reject := FailClosedTLSConfig()
	reject.RootCAs = trusted.RootCAs
	dialer.Config = reject
	conn, err = dialer.DialContext(ctx, "tcp", address)
	require.ErrorContains(t, err, "requested TLS configuration is unavailable")
	require.Nil(t, conn)
}
