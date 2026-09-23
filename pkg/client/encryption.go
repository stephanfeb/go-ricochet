package client

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"

	"filippo.io/edwards25519"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/crypto/nacl/box"

	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

const nonceSize = 24

// boundMagic opens a ciphertext that carries a Binding. A legacy ciphertext
// starts with a random nonce, which begins with these four bytes with
// probability 2^-32; DecryptBoundPayload falls back to the legacy format
// when the bound one does not open, so that collision costs one failed
// attempt rather than a message.
const boundMagic = "RCE2"

// Binding is what a ciphertext is tied to. It is sealed inside the box
// alongside the payload and checked on decrypt, so a ciphertext lifted
// from one message cannot be replayed as another: the server, or anyone
// who can write into a mailbox, cannot move it to a different folder or
// present it under a different message id or to a different recipient
// without the recipient noticing.
//
// It does not detect a replay of the same ciphertext under the same
// identity into the same folder, for instance after the recipient deleted
// it; only a record of seen message ids on the client can.
type Binding struct {
	RecipientPeerID string
	FolderPath      string
	MessageID       string
}

// BindingFor is the binding of a message as it will be, or was, submitted:
// an empty folder path is the inbox, which is where the server delivers it.
func BindingFor(msg *wire.Message) Binding {
	folder := msg.FolderPath
	if folder == "" {
		folder = "inbox"
	}
	return Binding{RecipientPeerID: msg.RecipientPeerID, FolderPath: folder, MessageID: msg.MessageID}
}

// header renders the binding as length-prefixed fields.
func (b Binding) header() []byte {
	fields := [][]byte{[]byte(b.RecipientPeerID), []byte(b.FolderPath), []byte(b.MessageID)}
	size := 0
	for _, f := range fields {
		size += 2 + len(f)
	}
	out := make([]byte, 0, size)
	for _, f := range fields {
		out = binary.BigEndian.AppendUint16(out, uint16(len(f)))
		out = append(out, f...)
	}
	return out
}

// splitHeader separates a sealed plaintext into its binding and payload.
func splitHeader(plain []byte) (Binding, []byte, error) {
	var fields [3]string
	for i := range fields {
		if len(plain) < 2 {
			return Binding{}, nil, fmt.Errorf("bound payload header truncated")
		}
		n := int(binary.BigEndian.Uint16(plain))
		plain = plain[2:]
		if len(plain) < n {
			return Binding{}, nil, fmt.Errorf("bound payload header truncated")
		}
		fields[i] = string(plain[:n])
		plain = plain[n:]
	}
	return Binding{RecipientPeerID: fields[0], FolderPath: fields[1], MessageID: fields[2]}, plain, nil
}

// maxBindingField bounds each header field so the two-byte length prefix
// cannot be exceeded. Peer ids, folder paths and message ids are all far
// shorter in practice.
const maxBindingField = 1<<16 - 1

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

// EncryptBoundPayload seals payload for recipient, tied to b.
//
// Wire format: "RCE2" [24-byte nonce] [box(header || payload)], where the
// box is NaCl box (X25519 key agreement, XSalsa20-Poly1305) between the
// sender's identity key and the recipient's, and the header is the binding
// as three length-prefixed fields. This is what the client sends; see
// DecryptBoundPayload for what it accepts.
func EncryptBoundPayload(payload []byte, b Binding, recipientPeerID peer.ID, senderPrivKey crypto.PrivKey) ([]byte, wire.SFMessageFlags, error) {
	for _, f := range []string{b.RecipientPeerID, b.FolderPath, b.MessageID} {
		if len(f) > maxBindingField {
			return nil, 0, fmt.Errorf("binding field exceeds %d bytes", maxBindingField)
		}
	}
	header := b.header()
	plain := make([]byte, 0, len(header)+len(payload))
	plain = append(plain, header...)
	plain = append(plain, payload...)

	sealed, flags, err := EncryptPayload(plain, recipientPeerID, senderPrivKey)
	if err != nil {
		return nil, 0, err
	}
	out := make([]byte, 0, len(boundMagic)+len(sealed))
	out = append(out, boundMagic...)
	out = append(out, sealed...)
	return out, flags, nil
}

// DecryptBoundPayload opens a ciphertext and checks that it was sealed for
// exactly the message it arrived as. It returns the payload and whether the
// ciphertext carried a binding: a ciphertext in the legacy unbound format
// still opens, so messages sealed before bindings existed remain readable,
// but a caller that wants the replay protection should treat bound == false
// as a downgrade.
func DecryptBoundPayload(encrypted []byte, want Binding, senderPeerID peer.ID, recipientPrivKey crypto.PrivKey) (payload []byte, bound bool, err error) {
	if len(encrypted) > len(boundMagic) && string(encrypted[:len(boundMagic)]) == boundMagic {
		plain, err := DecryptPayload(encrypted[len(boundMagic):], senderPeerID, recipientPrivKey)
		if err == nil {
			got, payload, err := splitHeader(plain)
			if err != nil {
				return nil, true, err
			}
			if got != want {
				return nil, true, fmt.Errorf("ciphertext is bound to another message (recipient %q, folder %q, id %q)",
					got.RecipientPeerID, got.FolderPath, got.MessageID)
			}
			return payload, true, nil
		}
		// A legacy nonce that happens to begin with the magic falls through.
	}
	plain, err := DecryptPayload(encrypted, senderPeerID, recipientPrivKey)
	if err != nil {
		return nil, false, err
	}
	return plain, false, nil
}

// EncryptPayload encrypts a plaintext payload using NaCl box
// (X25519 key agreement + XSalsa20-Poly1305 authenticated encryption).
//
// Wire format: [24-byte nonce][ciphertext+poly1305 tag]
//
// The sender's Ed25519 private key and the recipient's peer ID are used to
// derive the shared secret via X25519.
//
// This is the legacy, unbound format: nothing ties the ciphertext to the
// message that carries it. The client sends EncryptBoundPayload and only
// accepts this format for messages sealed before bindings existed.
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
