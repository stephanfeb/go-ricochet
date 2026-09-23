package wire

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/mda/mailboxes"
)

// The handlers used to check ownership each in their own way and answer each
// in their own shape: a bare map with an "error" key, an ack with a message
// and no status, a 403 with a header. Every refusal now comes from one of the
// two checks below and is a mailboxes.UnauthorizedError, which Classify turns
// into a 403 carrying the reason in whichever envelope the protocol uses.

// Forbidden reports a refused authorization. The reason reaches the client
// verbatim, prefixed "unauthorized:", so it must not say anything about the
// resource the caller was not allowed to see.
func Forbidden(reason string) error {
	return &mailboxes.UnauthorizedError{Message: reason}
}

// RequireOwner refuses a caller who is not the owner of the resource. The
// action names what was attempted, for the client's message.
func RequireOwner(sc *forge.StreamContext, owner peer.ID, action string) error {
	if sc.PeerID == owner {
		return nil
	}
	return Forbidden(fmt.Sprintf("%s require owner access", action))
}

// RequireSelf refuses a request that names an identity other than the
// connection's peer. An empty claim means the caller, which is what every
// field that takes a peer ID defaults to.
//
// The check is against the authenticated peer rather than the claimed one
// because the claim is just a string in the request: a sender, an owner, an
// expunge target. What the connection proved is the only identity the
// server can act on.
func RequireSelf(sc *forge.StreamContext, claimed, field string) error {
	if claimed == "" || claimed == sc.PeerID.String() {
		return nil
	}
	return Forbidden(fmt.Sprintf("%s must be the connected peer", field))
}
