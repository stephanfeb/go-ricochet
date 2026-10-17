package wire_test

import (
	"strings"
	"testing"

	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/protocol/wire"
)

// A banned peer may read its own trust-safety folder and clear what it
// read, the way pkg/client asks (MarkDeliveredInFolder names the folder),
// and nothing else.
func TestBannedMayAccessOnlyItsTrustSafetyFolder(t *testing.T) {
	me, other := newPeer(t), newPeer(t)
	cases := []struct {
		req  string
		want bool
	}{
		{`{"operationType":"retrieve","peerId":"{me}","folderPath":"trust-safety"}`, true},
		{`{"peerId":"{me}","folderPath":"trust-safety"}`, true},
		{`{"operationType":"retrieve","folderPath":"trust-safety"}`, true},
		{`{"operationType":"retrieve","peerId":"{other}","folderPath":"trust-safety"}`, false},
		{`{"operationType":"retrieve","peerId":"{me}","folderPath":"inbox"}`, false},
		{`{"operationType":"markDelivered","messageIds":["m1"],"folderPath":"trust-safety"}`, true},
		{`{"operationType":"markDelivered","messageIds":["m1"]}`, false},
		{`{"operationType":"deleteMessages","messageIds":["m1"]}`, true},
		{`{"operationType":"updateFlags","messageId":"m1","folderPath":"trust-safety"}`, false},
		{`{"operationType":"expunge","folderPath":"trust-safety"}`, false},
		{`not json`, false},
	}
	for _, c := range cases {
		req := strings.NewReplacer("{me}", me.String(), "{other}", other.String()).Replace(c.req)
		sc := &forge.StreamContext{PeerID: me, RawBytes: []byte(req)}
		if got := wire.BannedMayAccess(sc); got != c.want {
			t.Errorf("%s: got %v, want %v", req, got, c.want)
		}
	}
}
