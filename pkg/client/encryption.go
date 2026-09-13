package client

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"io"

	"filippo.io/edwards25519"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/crypto/nacl/box"

	"github.com/twostack/go-ricochet/pkg/wire"
)

const nonceSize = 24

// Ed25519PrivateKeyToX25519 converts an Ed25519 private key to an X25519
// private key. It uses the standard conversion: SHA-512 hash of the seed,
// clamped per RFC 7748.
func Ed25519PrivateKeyToX25519(edPriv ed25519.PrivateKey) [32]byte {
	h := sha512.Sum512(edPriv.Seed())
	var x [32]byte
	copy(x[:], h[:32])
	// Clamp per RFC 7748.
	x[0] &= 248
	x[31] &= 127
	x[31] |= 64
	return x
}

// Ed25519PublicKeyToX25519 converts an Ed25519 public key to an X25519
// public key using the birational map from Edwards to Montgomery form.
func Ed25519PublicKeyToX25519(edPub ed25519.PublicKey) ([32]byte, error) {
	p, err := new(edwards25519.Point).SetBytes(edPub)
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid ed25519 public key: %w", err)
	}
	mb := p.BytesMontgomery()
	var x [32]byte
	copy(x[:], mb)
	return x, nil
}

// libp2pKeyToEd25519 extracts the standard library ed25519 private key from
// a libp2p crypto private key. Only Ed25519 keys are supported.
func libp2pKeyToEd25519(privKey crypto.PrivKey) (ed25519.PrivateKey, error) {
	if privKey.Type() != crypto.Ed25519 {
		return nil, fmt.Errorf("unsupported key type: %d (only Ed25519 supported)", privKey.Type())
	}
	raw, err := privKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("extract raw private key: %w", err)
	}
	// libp2p Ed25519 Raw() returns the full 64-byte private key.
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("unexpected ed25519 key size: %d", len(raw))
	}
	return ed25519.PrivateKey(raw), nil
}

// PeerIDToX25519PublicKey extracts the Ed25519 public key embedded in a
// libp2p peer ID and converts it to its X25519 equivalent.
func PeerIDToX25519PublicKey(id peer.ID) ([32]byte, error) {
	pubKey, err := id.ExtractPublicKey()
	if err != nil {
		return [32]byte{}, fmt.Errorf("extract public key from peer ID: %w", err)
	}
	raw, err := pubKey.Raw()
	if err != nil {
		return [32]byte{}, fmt.Errorf("extract raw public key: %w", err)
	}
	return Ed25519PublicKeyToX25519(ed25519.PublicKey(raw))
}

// EncryptPayload encrypts a plaintext payload using NaCl box
// (X25519 key agreement + XSalsa20-Poly1305 authenticated encryption).
//
// Wire format: [24-byte nonce][ciphertext+poly1305 tag]
//
// The sender's Ed25519 private key and the recipient's peer ID are used to
// derive the shared secret via X25519.
func EncryptPayload(payload []byte, recipientPeerID peer.ID, senderPrivKey crypto.PrivKey) ([]byte, wire.SFMessageFlags, error) {
	// Convert sender's private key to X25519.
	edPriv, err := libp2pKeyToEd25519(senderPrivKey)
	if err != nil {
		return nil, 0, err
	}
	senderX := Ed25519PrivateKeyToX25519(edPriv)

	// Convert recipient's public key to X25519.
	recipientX, err := PeerIDToX25519PublicKey(recipientPeerID)
	if err != nil {
		return nil, 0, fmt.Errorf("convert recipient key: %w", err)
	}

	// Generate random nonce.
	var nonce [nonceSize]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, 0, fmt.Errorf("generate nonce: %w", err)
	}

	// Encrypt: prepend nonce, then box.Seal appends ciphertext.
	encrypted := box.Seal(nonce[:], payload, &nonce, &recipientX, &senderX)

	return encrypted, wire.FlagEncrypted, nil
}

// DecryptPayload decrypts a NaCl box encrypted payload.
//
// The sender's peer ID is used to derive their X25519 public key, and the
// recipient's Ed25519 private key is converted to X25519 for decryption.
func DecryptPayload(encrypted []byte, senderPeerID peer.ID, recipientPrivKey crypto.PrivKey) ([]byte, error) {
	if len(encrypted) < nonceSize+box.Overhead {
		return nil, fmt.Errorf("encrypted payload too short: %d bytes", len(encrypted))
	}

	// Extract nonce from the first 24 bytes.
	var nonce [nonceSize]byte
	copy(nonce[:], encrypted[:nonceSize])

	// Convert sender's public key to X25519.
	senderX, err := PeerIDToX25519PublicKey(senderPeerID)
	if err != nil {
		return nil, fmt.Errorf("convert sender key: %w", err)
	}

	// Convert recipient's private key to X25519.
	edPriv, err := libp2pKeyToEd25519(recipientPrivKey)
	if err != nil {
		return nil, err
	}
	recipientX := Ed25519PrivateKeyToX25519(edPriv)

	// Decrypt.
	plaintext, ok := box.Open(nil, encrypted[nonceSize:], &nonce, &senderX, &recipientX)
	if !ok {
		return nil, fmt.Errorf("decryption failed (wrong key or corrupted data)")
	}

	return plaintext, nil
}
