package cryptutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
)

// FailClosedTLSConfig keeps TLS enabled when requested credentials cannot be
// loaded. Returning nil would silently switch Redis clients to plaintext.
// No peer is accepted, even if a caller later supplies trusted roots.
func FailClosedTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    x509.NewCertPool(),
		VerifyConnection: func(tls.ConnectionState) error {
			return errors.New("requested TLS configuration is unavailable; refusing connection")
		},
	}
}
