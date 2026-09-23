package mailboxes

import (
	"context"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// Page bounds for retrieval. A request that names no size gets the default;
// one that asks for more than the maximum is clamped, not refused.
const (
	DefaultPageSize = 100
	MaxPageSize     = 1000
)

// RetrieveOpts holds options for message retrieval.
type RetrieveOpts struct {
	FromSequence *int
	MaxMessages  *int
	MinPriority  *core.MessagePriority

	// MaxBytes bounds the encoded size of the page. Zero means unbounded.
	// The access protocol returns a page as one frame, and a frame has a
	// hard cap, so a page must be cut to fit before anything downstream
	// commits to it — a cursor advanced past a message the frame then
	// could not carry would lose that message for good.
	MaxBytes int
}

// Mailbox defines the interface for all mailbox types.
type Mailbox interface {
	// StoreMessage stores a message in this mailbox.
	StoreMessage(ctx context.Context, msg *core.Message) error

	// RetrieveMessages returns one page of messages and whether more
	// remain past it. It never removes anything: a message leaves a
	// mailbox when its owner marks it delivered, deletes it, or lets it
	// expire, not because it was read.
	RetrieveMessages(ctx context.Context, readerID *peer.ID, opts RetrieveOpts) (msgs []*core.Message, hasMore bool, err error)

	// Record returns the underlying mailbox record.
	Record() *storage.MailboxRecord
}

// UnauthorizedError indicates an access control failure.
type UnauthorizedError struct {
	Message string
}

func (e *UnauthorizedError) Error() string {
	return fmt.Sprintf("unauthorized: %s", e.Message)
}

// NotFoundError indicates the addressed mailbox does not exist.
//
// It is distinct from UnauthorizedError on purpose: a reader who is refused
// an existing mailbox and a reader who names one that was never created get
// different codes, but neither learns anything about the other's contents.
type NotFoundError struct {
	Path string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("mailbox not found: %s", e.Path)
}

// MailboxFullError indicates a mailbox has reached capacity. Storage raises
// it, under the mailbox row lock, so it is the storage type by another name.
type MailboxFullError = storage.MailboxFullError

// storeMessage writes msg into the mailbox, letting storage enforce the cap.
//
// A mailbox with a retention_count is a rolling window: the sweep keeps only
// its newest retention_count messages. Pruning used to run after every store
// into a public mailbox, two DELETEs with a sort per message, so the window
// never touched the cap. It now runs in maintenance, and a window that fills
// the cap between sweeps is pruned here, on the full path only, and the store
// tried once more. The hot path pays nothing for it.
func storeMessage(ctx context.Context, store storage.Storage, record *storage.MailboxRecord, msg *core.Message) error {
	_, err := store.StoreMessage(ctx, record, msg)
	var full *MailboxFullError
	if !errors.As(err, &full) || record.RetentionCount == nil {
		return err
	}
	if perr := store.EnforceRetentionPolicy(ctx, record); perr != nil {
		return fmt.Errorf("prune full mailbox: %w", perr)
	}
	_, err = store.StoreMessage(ctx, record, msg)
	return err
}

// fetchPage reads one page from storage and reports whether more follow.
//
// It asks for one row past the limit to learn whether there is a next page
// without a second query, then trims to the count and byte budgets. At least
// one message is always kept, even if it alone exceeds the byte budget: a
// message that can never be paged would otherwise be stuck in front of
// everything behind it, and the budget is the frame cap that the same
// message already fitted under when it was submitted.
func fetchPage(ctx context.Context, store storage.Storage, record *storage.MailboxRecord, from *int, opts RetrieveOpts) ([]*core.Message, bool, error) {
	limit := DefaultPageSize
	if opts.MaxMessages != nil && *opts.MaxMessages > 0 {
		limit = *opts.MaxMessages
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}

	probe := limit + 1
	messages, err := store.RetrieveMessages(ctx, record, from, &probe, opts.MinPriority)
	if err != nil {
		return nil, false, err
	}

	hasMore := false
	if len(messages) > limit {
		messages = messages[:limit]
		hasMore = true
	}

	if opts.MaxBytes > 0 {
		kept, truncated, err := trimToBytes(messages, opts.MaxBytes)
		if err != nil {
			return nil, false, err
		}
		messages = kept
		hasMore = hasMore || truncated
	}

	return messages, hasMore, nil
}

// trimToBytes keeps the longest prefix whose encoded size fits the budget,
// counting the 4-byte length prefix each message carries inside the compound
// frame. The first message is kept unconditionally; see fetchPage.
func trimToBytes(messages []*core.Message, maxBytes int) ([]*core.Message, bool, error) {
	total := 0
	for i, msg := range messages {
		encoded, err := msg.ToJSON()
		if err != nil {
			return nil, false, fmt.Errorf("size message %s: %w", msg.MessageID, err)
		}
		total += 4 + len(encoded)
		if total > maxBytes && i > 0 {
			return messages[:i], true, nil
		}
	}
	return messages, false, nil
}

// QuotaExceededError refuses a create that would take an owner, or the
// server, past a configured count. Like MailboxFullError it is a 507: the
// condition clears only when something is deleted, so a client that retries
// on a timer is wasting both ends' time.
type QuotaExceededError struct {
	What    string // "mailboxes per owner", "mailboxes", "entries per feed"
	Current int
	Max     int
}

func (e *QuotaExceededError) Error() string {
	return fmt.Sprintf("quota exceeded: %s %d/%d", e.What, e.Current, e.Max)
}

// InvalidPathError refuses a folder path that fails validation. It is a
// 400: the sender chose the path and can choose another.
type InvalidPathError struct {
	Path   string
	Reason error
}

func (e *InvalidPathError) Error() string {
	return fmt.Sprintf("invalid folder path %q: %v", e.Path, e.Reason)
}

func (e *InvalidPathError) Unwrap() error { return e.Reason }
