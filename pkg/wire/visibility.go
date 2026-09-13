package wire

import (
	"fmt"
	"strings"
)

// Visibility says who may read a document, feed or collection. It is the
// mailbox model applied to the other stores: the owner always reads; a
// shared resource is also read by the peers on its reader list; a public one
// by anyone. Writes stay the owner's regardless of visibility.
//
// A resource's visibility covers the whole of it: every version of a
// document, every entry of a feed, every item of a collection.
type Visibility uint8

const (
	VisibilityPrivate Visibility = 0
	VisibilityShared  Visibility = 1
	VisibilityPublic  Visibility = 2
)

var visibilityNames = map[Visibility]string{
	VisibilityPrivate: "private",
	VisibilityShared:  "shared",
	VisibilityPublic:  "public",
}

func (v Visibility) String() string {
	if name, ok := visibilityNames[v]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", uint8(v))
}

// Valid reports whether v is one of the three visibilities.
func (v Visibility) Valid() bool {
	_, ok := visibilityNames[v]
	return ok
}

// VisibilityFromString parses a visibility name, case-insensitively.
func VisibilityFromString(s string) (Visibility, error) {
	switch strings.ToLower(s) {
	case "private":
		return VisibilityPrivate, nil
	case "shared":
		return VisibilityShared, nil
	case "public":
		return VisibilityPublic, nil
	default:
		return 0, fmt.Errorf("unknown visibility %q (want private, shared or public)", s)
	}
}

// StoreReader is one entry on a document's, feed's or collection's reader
// list: a peer granted read access while the resource is shared.
type StoreReader struct {
	PeerID    string `json:"peerId"`
	GrantedAt int64  `json:"grantedAt"` // Unix milliseconds
}

// StoreAccess is the ACCESS operation's reply for a document, feed or
// collection: its visibility and, for the owner, its reader list.
type StoreAccess struct {
	Visibility string        `json:"visibility"`
	Readers    []StoreReader `json:"readers"`
}
