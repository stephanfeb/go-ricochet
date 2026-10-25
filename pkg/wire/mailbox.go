package wire

import (
	"fmt"
	"strings"
)

// MailboxType represents the type of a mailbox.
type MailboxType uint8

const (
	MailboxPrivate MailboxType = 0
	MailboxShared  MailboxType = 1
	MailboxPublic  MailboxType = 2
	// MailboxClaim holds items its owner hands out one at a time: only the
	// owner stores or lists, and anyone may claim, which returns the oldest
	// unexpired message and deletes it in the same step. Signal one-time
	// prekeys live here, so that no two senders are given the same one.
	MailboxClaim MailboxType = 3
)

var mailboxTypeNames = map[MailboxType]string{
	MailboxPrivate: "private",
	MailboxShared:  "shared",
	MailboxPublic:  "public",
	MailboxClaim:   "claim",
}

func (t MailboxType) String() string {
	if name, ok := mailboxTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", uint8(t))
}

func MailboxTypeFromString(s string) (MailboxType, error) {
	switch strings.ToLower(s) {
	case "private":
		return MailboxPrivate, nil
	case "shared":
		return MailboxShared, nil
	case "public":
		return MailboxPublic, nil
	case "claim":
		return MailboxClaim, nil
	default:
		return 0, fmt.Errorf("unknown MailboxType: %s", s)
	}
}

// AccessMode represents the access mode for a mailbox ACL entry.
type AccessMode uint8

const (
	AccessReadOnly  AccessMode = 0
	AccessWriteOnly AccessMode = 1
	AccessReadWrite AccessMode = 2
)

var accessModeNames = map[AccessMode]string{
	AccessReadOnly:  "readOnly",
	AccessWriteOnly: "writeOnly",
	AccessReadWrite: "readWrite",
}

func (m AccessMode) String() string {
	if name, ok := accessModeNames[m]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", uint8(m))
}

func AccessModeFromString(s string) (AccessMode, error) {
	switch strings.ToLower(s) {
	case "readonly", "read_only":
		return AccessReadOnly, nil
	case "writeonly", "write_only":
		return AccessWriteOnly, nil
	case "readwrite", "read_write":
		return AccessReadWrite, nil
	default:
		return 0, fmt.Errorf("unknown AccessMode: %s", s)
	}
}
