package core

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func generateMailboxTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}
	return pid
}

func TestMailboxTypeFromString(t *testing.T) {
	tests := []struct {
		input   string
		want    MailboxType
		wantErr bool
	}{
		{"private", MailboxPrivate, false},
		{"shared", MailboxShared, false},
		{"public", MailboxPublic, false},
		{"Private", MailboxPrivate, false}, // case insensitive
		{"SHARED", MailboxShared, false},
		{"invalid", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := MailboxTypeFromString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("MailboxTypeFromString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("MailboxTypeFromString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestAccessModeFromString(t *testing.T) {
	tests := []struct {
		input   string
		want    AccessMode
		wantErr bool
	}{
		{"readonly", AccessReadOnly, false},
		{"writeonly", AccessWriteOnly, false},
		{"readwrite", AccessReadWrite, false},
		{"read_only", AccessReadOnly, false},
		{"write_only", AccessWriteOnly, false},
		{"read_write", AccessReadWrite, false},
		{"ReadOnly", AccessReadOnly, false}, // case insensitive
		{"invalid", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := AccessModeFromString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("AccessModeFromString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("AccessModeFromString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNewMailboxAddress(t *testing.T) {
	ownerID := generateMailboxTestPeerID(t)

	// Valid path
	addr, err := NewMailboxAddress(ownerID, "inbox", MailboxPrivate)
	if err != nil {
		t.Fatalf("NewMailboxAddress with valid path failed: %v", err)
	}
	if addr.OwnerID != ownerID {
		t.Errorf("OwnerID = %v, want %v", addr.OwnerID, ownerID)
	}
	if addr.FolderPath != "inbox" {
		t.Errorf("FolderPath = %q, want %q", addr.FolderPath, "inbox")
	}
	if addr.Type != MailboxPrivate {
		t.Errorf("Type = %v, want MailboxPrivate", addr.Type)
	}

	// Empty string should fail
	_, err = NewMailboxAddress(ownerID, "", MailboxPrivate)
	if err == nil {
		t.Error("NewMailboxAddress with empty path should fail")
	}

	// Leading slash should fail
	_, err = NewMailboxAddress(ownerID, "/inbox", MailboxPrivate)
	if err == nil {
		t.Error("NewMailboxAddress with leading slash should fail")
	}

	// Trailing slash should fail
	_, err = NewMailboxAddress(ownerID, "inbox/", MailboxPrivate)
	if err == nil {
		t.Error("NewMailboxAddress with trailing slash should fail")
	}

	// Double slash should fail
	_, err = NewMailboxAddress(ownerID, "in//box", MailboxPrivate)
	if err == nil {
		t.Error("NewMailboxAddress with double slash should fail")
	}
}

func TestNewInbox(t *testing.T) {
	ownerID := generateMailboxTestPeerID(t)

	inbox := NewInbox(ownerID)

	if inbox.OwnerID != ownerID {
		t.Errorf("OwnerID = %v, want %v", inbox.OwnerID, ownerID)
	}
	if inbox.FolderPath != "inbox" {
		t.Errorf("FolderPath = %q, want %q", inbox.FolderPath, "inbox")
	}
	if inbox.Type != MailboxPrivate {
		t.Errorf("Type = %v, want MailboxPrivate", inbox.Type)
	}
}

func TestMailboxAddress_FullPath(t *testing.T) {
	ownerID := generateMailboxTestPeerID(t)

	addr, err := NewMailboxAddress(ownerID, "inbox", MailboxPrivate)
	if err != nil {
		t.Fatalf("NewMailboxAddress failed: %v", err)
	}

	fullPath := addr.FullPath()
	expected := ownerID.String() + "/inbox"
	if fullPath != expected {
		t.Errorf("FullPath() = %q, want %q", fullPath, expected)
	}

	// Also verify String() returns the same
	if addr.String() != expected {
		t.Errorf("String() = %q, want %q", addr.String(), expected)
	}
}

func TestParseMailboxAddress(t *testing.T) {
	ownerID := generateMailboxTestPeerID(t)

	// Valid parse
	input := ownerID.String() + "/inbox"
	addr, err := ParseMailboxAddress(input, MailboxPrivate)
	if err != nil {
		t.Fatalf("ParseMailboxAddress(%q) error: %v", input, err)
	}
	if addr.OwnerID != ownerID {
		t.Errorf("OwnerID = %v, want %v", addr.OwnerID, ownerID)
	}
	if addr.FolderPath != "inbox" {
		t.Errorf("FolderPath = %q, want %q", addr.FolderPath, "inbox")
	}

	// No separator should fail
	_, err = ParseMailboxAddress("noseparator", MailboxPrivate)
	if err == nil {
		t.Error("ParseMailboxAddress without separator should fail")
	}

	// Invalid peerID should fail
	_, err = ParseMailboxAddress("invalidpeerid/inbox", MailboxPrivate)
	if err == nil {
		t.Error("ParseMailboxAddress with invalid peerID should fail")
	}

	// Nested folder path
	nestedInput := ownerID.String() + "/inbox/subfolder"
	nestedAddr, err := ParseMailboxAddress(nestedInput, MailboxShared)
	if err != nil {
		t.Fatalf("ParseMailboxAddress with nested path error: %v", err)
	}
	// The first "/" separates peerID from folder path, so folder path should be "inbox/subfolder"
	if !strings.HasPrefix(nestedAddr.FolderPath, "inbox") {
		t.Errorf("FolderPath = %q, expected to start with 'inbox'", nestedAddr.FolderPath)
	}
}
