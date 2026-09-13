package client

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/pkg/wire"
)

func generateTestKeyPair(t *testing.T) (libp2pcrypto.PrivKey, peer.ID) {
	t.Helper()
	priv, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return priv, id
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	senderPriv, senderID := generateTestKeyPair(t)
	recipientPriv, recipientID := generateTestKeyPair(t)

	payload := []byte("hello encrypted world")

	encrypted, flags, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if flags&wire.FlagEncrypted == 0 {
		t.Fatal("expected FlagEncrypted to be set")
	}
	if bytes.Equal(encrypted, payload) {
		t.Fatal("encrypted data should differ from plaintext")
	}

	decrypted, err := DecryptPayload(encrypted, senderID, recipientPriv)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, payload) {
		t.Errorf("round-trip mismatch: got %q, want %q", decrypted, payload)
	}
}

func TestEncryptDecryptEmptyPayload(t *testing.T) {
	senderPriv, senderID := generateTestKeyPair(t)
	recipientPriv, recipientID := generateTestKeyPair(t)

	payload := []byte{}

	encrypted, flags, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if flags&wire.FlagEncrypted == 0 {
		t.Fatal("expected FlagEncrypted to be set")
	}

	decrypted, err := DecryptPayload(encrypted, senderID, recipientPriv)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, payload) {
		t.Errorf("round-trip mismatch for empty payload: got %q, want %q", decrypted, payload)
	}
}

func TestEncryptDecryptLargePayload(t *testing.T) {
	senderPriv, senderID := generateTestKeyPair(t)
	recipientPriv, recipientID := generateTestKeyPair(t)

	payload := make([]byte, 64*1024) // 64KB
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	encrypted, _, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	decrypted, err := DecryptPayload(encrypted, senderID, recipientPriv)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, payload) {
		t.Error("round-trip mismatch for large payload")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	senderPriv, senderID := generateTestKeyPair(t)
	_, recipientID := generateTestKeyPair(t)
	wrongPriv, _ := generateTestKeyPair(t)

	payload := []byte("secret message")

	encrypted, _, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	_, err = DecryptPayload(encrypted, senderID, wrongPriv)
	if err == nil {
		t.Fatal("expected decryption to fail with wrong key")
	}
}

func TestDecryptWrongSender(t *testing.T) {
	senderPriv, _ := generateTestKeyPair(t)
	recipientPriv, recipientID := generateTestKeyPair(t)
	_, wrongSenderID := generateTestKeyPair(t)

	payload := []byte("secret message")

	encrypted, _, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	_, err = DecryptPayload(encrypted, wrongSenderID, recipientPriv)
	if err == nil {
		t.Fatal("expected decryption to fail with wrong sender")
	}
}

func TestPeerIDToX25519PublicKey(t *testing.T) {
	_, id := generateTestKeyPair(t)
	x25519Pub, err := PeerIDToX25519PublicKey(id)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	var zero [32]byte
	if x25519Pub == zero {
		t.Error("X25519 public key is all zeros")
	}
}

func TestEd25519KeyConversion(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	x25519Priv := Ed25519PrivateKeyToX25519(priv)
	x25519Pub, err := Ed25519PublicKeyToX25519(pub)
	if err != nil {
		t.Fatalf("public key conversion: %v", err)
	}

	var zero [32]byte
	if x25519Priv == zero {
		t.Error("X25519 private key is all zeros")
	}
	if x25519Pub == zero {
		t.Error("X25519 public key is all zeros")
	}
}

func TestDecryptPayloadTooShort(t *testing.T) {
	senderPriv, senderID := generateTestKeyPair(t)
	_ = senderPriv
	_, err := DecryptPayload([]byte{1, 2, 3}, senderID, senderPriv)
	if err == nil {
		t.Error("expected error for too-short encrypted payload")
	}
}

func TestEncryptDecryptDeterministicKeys(t *testing.T) {
	// Verify that the same keys always produce decryptable output.
	senderPriv, senderID := generateTestKeyPair(t)
	recipientPriv, recipientID := generateTestKeyPair(t)

	payload := []byte("deterministic test")

	// Encrypt twice - should produce different ciphertexts (random nonce).
	enc1, _, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatal(err)
	}
	enc2, _, err := EncryptPayload(payload, recipientID, senderPriv)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(enc1, enc2) {
		t.Error("two encryptions of the same data should produce different ciphertexts")
	}

	// Both should decrypt correctly.
	dec1, err := DecryptPayload(enc1, senderID, recipientPriv)
	if err != nil {
		t.Fatal(err)
	}
	dec2, err := DecryptPayload(enc2, senderID, recipientPriv)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dec1, payload) || !bytes.Equal(dec2, payload) {
		t.Error("both decryptions should match the original payload")
	}
}
