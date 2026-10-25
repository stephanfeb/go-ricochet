package mda

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/mda/mailboxes"
)

// DefaultClaimsPerHour bounds how fast one claim mailbox can be emptied.
//
// Peer IDs cost nothing to mint, so a per-claimer limit would stop no one: a
// stranger could take every one-time prekey a peer published. The bound is
// per mailbox instead. Past it a claim finds nothing, which is what the
// claimer would see if the owner had run out, and Signal senders handle that
// by agreeing a session without a one-time prekey.
const DefaultClaimsPerHour = 30

// claimLimiters holds a token bucket per claim mailbox, bounded so that a
// claimer naming many mailboxes cannot grow it without limit. A mailbox
// evicted from it starts again with a full bucket, which only matters for
// mailboxes nobody has claimed from in a while.
type claimLimiters struct {
	once    sync.Once
	perHour int
	cache   *lru.Cache[int64, *rate.Limiter]
	mu      sync.Mutex
}

func (l *claimLimiters) allow(mailboxID int64) bool {
	l.once.Do(func() {
		if l.perHour <= 0 {
			l.perHour = DefaultClaimsPerHour
		}
		l.cache, _ = lru.New[int64, *rate.Limiter](DefaultMailboxCacheSize)
	})
	l.mu.Lock()
	lim, ok := l.cache.Get(mailboxID)
	if !ok {
		lim = rate.NewLimiter(rate.Every(time.Hour/time.Duration(l.perHour)), l.perHour)
		l.cache.Add(mailboxID, lim)
	}
	l.mu.Unlock()
	return lim.Allow()
}

// WithClaimsPerHour sets the per-mailbox claim bound; see
// DefaultClaimsPerHour. Call it before the server takes traffic.
func (s *MailboxServer) WithClaimsPerHour(n int) *MailboxServer {
	s.claims.perHour = n
	return s
}

// Claim takes one message from a claim mailbox for callerID: the oldest
// unexpired one, deleted as it is returned. It returns nil when the mailbox
// is empty or has handed out its share for now.
//
// Only claim mailboxes may be claimed from. Any other type is refused, so a
// claim cannot be used to delete a public mailbox's posts.
func (s *MailboxServer) Claim(ctx context.Context, addr *core.MailboxAddress, callerID peer.ID) (*core.Message, error) {
	mb, err := s.findMailbox(ctx, addr)
	if err != nil {
		return nil, err
	}
	if mb == nil {
		return nil, &mailboxes.NotFoundError{Path: addr.FullPath()}
	}
	record := mb.Record()
	if record.Type != core.MailboxClaim {
		return nil, &mailboxes.UnauthorizedError{Message: "not a claim mailbox"}
	}

	if !s.claims.allow(record.ID) {
		s.logger.Debug("claim refused: mailbox over its claim rate",
			"mailbox", addr.FullPath(), "caller", callerID.String())
		return nil, nil
	}

	msg, err := s.Storage.ClaimMessage(ctx, record.ID)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("claimed message",
		"mailbox", addr.FullPath(),
		"caller", callerID.String(),
		"found", msg != nil,
	)
	return msg, nil
}
