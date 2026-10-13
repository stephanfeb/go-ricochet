package transports

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	forgehost "github.com/stephanfeb/go-p2p-forge/host"
)

// writeSelfSigned writes a certificate for 127.0.0.1 and its key, and
// returns the certificate's DER.
func writeSelfSigned(t *testing.T, certFile, keyFile string, serial int64) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "ricochet test"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return der
}

// The server listens for secure WebSocket with its own certificate, and a
// client that trusts it stores a stream through it.
func TestSecureWebSocketWithACertificate(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	der := writeSelfSigned(t, certFile, keyFile, 1)

	serverTLS, err := LoadServerTLS(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	priv, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	opts, err := Options([]string{WebSocket}, WebSocketServerTLS(serverTLS))
	if err != nil {
		t.Fatal(err)
	}
	cfg := forgehost.DefaultConfig()
	cfg.ListenAddresses = []string{"/ip4/127.0.0.1/tcp/0/tls/ws"}
	cfg.EnableRelay = false
	cfg.EnableAutoNAT = false
	server, err := forgehost.Create(cfg, priv, slog.New(slog.DiscardHandler), opts...)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	server.SetStreamHandler(echoProtocol, func(s network.Stream) {
		defer s.Close()
		io.Copy(s, s)
	})

	leaf, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	clientPriv, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	clientOpts, err := Options([]string{WebSocket}, WebSocketClientTLS(&tls.Config{RootCAs: roots}))
	if err != nil {
		t.Fatal(err)
	}
	ccfg := forgehost.DefaultConfig()
	ccfg.ListenAddresses = []string{"/ip4/127.0.0.1/tcp/0/ws"}
	ccfg.EnableRelay = false
	ccfg.EnableAutoNAT = false
	client, err := forgehost.Create(ccfg, clientPriv, slog.New(slog.DiscardHandler), clientOpts...)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}); err != nil {
		t.Fatalf("connect over wss to %v: %v", server.Addrs(), err)
	}
	s, err := client.NewStream(ctx, server.ID(), echoProtocol)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	s.Write([]byte("over tls"))
	s.CloseWrite()
	if got, _ := io.ReadAll(s); string(got) != "over tls" {
		t.Fatalf("echo = %q", got)
	}
	if !isSecureWebSocket(client.Network().ConnsToPeer(server.ID())[0].RemoteMultiaddr()) {
		t.Fatal("the connection is not over secure WebSocket")
	}
}

// A renewed certificate serves from the next handshake; a renewal that
// cannot be loaded leaves the previous one serving.
func TestServerTLSReloadsARenewedCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	first := writeSelfSigned(t, certFile, keyFile, 1)

	conf, err := LoadServerTLS(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	served := func() []byte {
		c, err := conf.GetCertificate(&tls.ClientHelloInfo{})
		if err != nil {
			t.Fatal(err)
		}
		return c.Certificate[0]
	}
	if !bytes.Equal(served(), first) {
		t.Fatal("not serving the loaded certificate")
	}

	second := writeSelfSigned(t, certFile, keyFile, 2)
	later := time.Now().Add(time.Minute)
	os.Chtimes(certFile, later, later) // a renewal within the file system's timestamp resolution
	if !bytes.Equal(served(), second) {
		t.Fatal("the renewed certificate is not served")
	}

	os.WriteFile(certFile, []byte("half-written"), 0o600)
	evenLater := later.Add(time.Minute)
	os.Chtimes(certFile, evenLater, evenLater)
	if !bytes.Equal(served(), second) {
		t.Fatal("a broken renewal replaced the serving certificate")
	}

	if _, err := LoadServerTLS(filepath.Join(dir, "missing.pem"), keyFile); err == nil {
		t.Fatal("a missing certificate loaded")
	}
}
