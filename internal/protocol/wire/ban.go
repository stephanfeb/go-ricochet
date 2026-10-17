package wire

import (
	"encoding/json"

	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/moderation"
)

// BannedMessage is what a banned peer is told, after "unauthorized: ". The
// word "banned" is what the app recognises: the peer is not offline, it is
// banned, and its notices are in its trust-safety folder.
const BannedMessage = "banned: a moderator banned this peer; only its trust-safety folder can be read"

// NoticesFolder is the folder Trust & Safety's notices arrive in
// (ricochet-mail doc/report-format.md); a banned peer may still read it.
const NoticesFolder = "trust-safety"

// Banned is the refusal a banned peer gets: a 403 carrying BannedMessage.
func Banned() error { return Forbidden(BannedMessage) }

// BanGate refuses a banned caller every request that allow does not let
// through (nil lets nothing through). It goes right after the frame is read.
func BanGate(allow func(sc *forge.StreamContext) bool) forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		if moderation.FromStream(sc).IsBanned(sc.PeerID) && (allow == nil || !allow(sc)) {
			sc.Err = Banned()
			return
		}
		next()
	}
}

// BannedMayAccess lets a banned peer read its own trust-safety folder and
// clear what it read: retrieve from it, mark delivered in it, and delete
// its own messages (delete names messages, not a folder, and a peer only
// ever deletes its own).
func BannedMayAccess(sc *forge.StreamContext) bool {
	var req struct {
		OperationType string `json:"operationType"`
		PeerID        string `json:"peerId"`
		FolderPath    string `json:"folderPath"`
	}
	if err := json.Unmarshal(sc.RawBytes, &req); err != nil {
		return false
	}
	switch req.OperationType {
	case "", "retrieve":
		return req.FolderPath == NoticesFolder && (req.PeerID == "" || req.PeerID == sc.PeerID.String())
	case "markDelivered":
		return req.FolderPath == NoticesFolder
	case "deleteMessages":
		return true
	}
	return false
}
