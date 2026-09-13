package core

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/libp2p/go-libp2p/core/peer"
)

// MailboxAddress represents a fully-qualified mailbox address.
type MailboxAddress struct {
	OwnerID    peer.ID
	FolderPath string
	Type       MailboxType
}

// NewMailboxAddress creates a new MailboxAddress with path validation.
func NewMailboxAddress(ownerID peer.ID, folderPath string, typ MailboxType) (*MailboxAddress, error) {
	if err := validateFolderPath(folderPath); err != nil {
		return nil, err
	}
	return &MailboxAddress{
		OwnerID:    ownerID,
		FolderPath: folderPath,
		Type:       typ,
	}, nil
}

// NewInbox creates a private inbox mailbox address.
func NewInbox(ownerID peer.ID) *MailboxAddress {
	return &MailboxAddress{
		OwnerID:    ownerID,
		FolderPath: "inbox",
		Type:       MailboxPrivate,
	}
}

// ParseMailboxAddress parses a "peerID/folder/path" string into a MailboxAddress.
func ParseMailboxAddress(s string, typ MailboxType) (*MailboxAddress, error) {
	idx := strings.Index(s, "/")
	if idx < 0 {
		return nil, fmt.Errorf("invalid mailbox address: missing folder path separator in %q", s)
	}

	peerIDStr := s[:idx]
	folderPath := s[idx+1:]

	ownerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid peer ID in mailbox address: %w", err)
	}

	return NewMailboxAddress(ownerID, folderPath, typ)
}

// FullPath returns the full "peerID/folder/path" string.
func (a *MailboxAddress) FullPath() string {
	return a.OwnerID.String() + "/" + a.FolderPath
}

func (a *MailboxAddress) String() string {
	return a.FullPath()
}

// MaxFolderPathLength bounds a folder path. Paths are index keys and are
// echoed in every listing, and a sender can name one on delivery, so an
// unbounded path was a way to store ten megabytes in the mailboxes table.
const MaxFolderPathLength = 256

func validateFolderPath(path string) error {
	if path == "" {
		return fmt.Errorf("folder path cannot be empty")
	}
	if len(path) > MaxFolderPathLength {
		return fmt.Errorf("folder path exceeds %d bytes", MaxFolderPathLength)
	}
	if !utf8.ValidString(path) {
		return fmt.Errorf("folder path is not valid UTF-8")
	}
	for _, r := range path {
		if unicode.IsControl(r) {
			return fmt.Errorf("folder path contains a control character")
		}
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("folder path cannot start with /")
	}
	if strings.HasSuffix(path, "/") {
		return fmt.Errorf("folder path cannot end with /")
	}
	if strings.Contains(path, "//") {
		return fmt.Errorf("folder path cannot contain //")
	}
	return nil
}
