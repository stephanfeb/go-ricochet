package transports

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"
)

// LoadServerTLS loads a certificate and key for listening on secure
// WebSocket, and reloads them when the certificate file changes, so a
// renewal (certbot, an ACME client) takes effect without a restart. A
// renewal that cannot be loaded leaves the previous certificate serving
// and is retried on later handshakes.
func LoadServerTLS(certFile, keyFile string) (*tls.Config, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile}
	if err := r.reload(); err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.get,
	}, nil
}

type certReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime time.Time
}

func (r *certReloader) reload() error {
	info, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("websocket certificate: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("websocket certificate: %w", err)
	}
	r.mu.Lock()
	r.cert, r.modTime = &cert, info.ModTime()
	r.mu.Unlock()
	return nil
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if info, err := os.Stat(r.certFile); err == nil {
		r.mu.Lock()
		changed := !info.ModTime().Equal(r.modTime)
		r.mu.Unlock()
		if changed {
			r.reload() // on failure the previous certificate keeps serving
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert, nil
}
