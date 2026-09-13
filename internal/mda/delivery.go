package mda

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda/mailboxes"
	"github.com/twostack/go-ricochet/internal/storage"
)

// RetrieveOpts holds options for message retrieval.
type RetrieveOpts struct {
	FromSequence *int
	MaxMessages  *int
	MinPriority  *core.MessagePriority
	// MaxBytes bounds the encoded page; see mailboxes.RetrieveOpts.
	MaxBytes int
}

// Fallbacks for a MailboxDefaults built without values. They mirror
// core.DefaultConfig, so a server assembled by hand behaves like a configured
// one rather than like something with no limits at all.
const (
	fallbackMaxMessages   = 1000
	fallbackRetentionDays = 30
)

// MailboxDefaults are the settings given to a mailbox created implicitly by
// message delivery.
//
// They exist as a type rather than as two arguments because both values are
// dangerous when zero and the guard belongs with them: a cap of zero makes
// every mailbox instantly full, and a retention of zero days deletes every
// message older than this instant on the next sweep.
type MailboxDefaults struct {
	MaxMessages   int
	RetentionDays int

	// MaxMailboxesPerOwner and MaxMailboxes are the quotas a create is
	// checked against. Zero means unlimited, which is only for hand-built
	// servers in tests; DefaultsFromConfig always sets both.
	MaxMailboxesPerOwner int
	MaxMailboxes         int
}

// DefaultsFromConfig derives the delivery-time defaults from the server
// configuration.
//
// This is the whole point of the type: before it, getMailbox passed the
// literals 1000 and 30, so max_messages_per_mailbox and retention_policy
// applied only to mailboxes somebody created explicitly through MMA. An
// operator could lower the cap, watch delivery keep filling mailboxes past it,
// and have nothing in the logs to explain why.
func DefaultsFromConfig(cfg *core.ServerConfig) MailboxDefaults {
	if cfg == nil {
		return MailboxDefaults{}.orFallbacks()
	}
	return MailboxDefaults{
		MaxMessages:          cfg.MaxMessagesPerMailbox,
		RetentionDays:        retentionDays(cfg.RetentionPolicy),
		MaxMailboxesPerOwner: cfg.MaxMailboxesPerOwner,
		MaxMailboxes:         cfg.MaxMailboxes,
	}.orFallbacks()
}

// retentionDays converts a retention duration to whole days.
//
// It rounds a positive policy up rather than down. Retention is enforced as
// "delete anything older than N days", so a twelve-hour policy truncating to
// zero would delete every message on the next sweep — turning a short
// retention window into immediate data loss. A policy shorter than a day
// becomes one day; expressing anything finer needs a different mechanism than
// a day count.
func retentionDays(policy time.Duration) int {
	if policy <= 0 {
		return 0 // caller substitutes the fallback
	}
	days := int(policy / (24 * time.Hour))
	if policy%(24*time.Hour) != 0 {
		days++
	}
	return days
}

// orFallbacks replaces values that would be actively harmful rather than
// merely unset.
func (d MailboxDefaults) orFallbacks() MailboxDefaults {
	if d.MaxMessages <= 0 {
		d.MaxMessages = fallbackMaxMessages
	}
	if d.RetentionDays <= 0 {
		d.RetentionDays = fallbackRetentionDays
	}
	return d
}

// MailboxServer is the Mail Delivery Agent — handles local message storage and retrieval.
type MailboxServer struct {
	Storage      storage.Storage
	mailboxCache map[string]mailboxes.Mailbox
	mu           sync.RWMutex
	logger       *slog.Logger
	notifier     *Notifier
	defaults     MailboxDefaults
}

// SetNotifier sets the push notification sender for the MDA.
func (s *MailboxServer) SetNotifier(n *Notifier) {
	s.notifier = n
}

// NewMailboxServer creates a new MDA.
//
// The defaults are a parameter rather than a setter so that a caller cannot
// forget them: forgetting is what the previous hardcoded literals amounted to,
// and it failed silently.
func NewMailboxServer(store storage.Storage, defaults MailboxDefaults, logger *slog.Logger) *MailboxServer {
	return &MailboxServer{
		Storage:      store,
		mailboxCache: make(map[string]mailboxes.Mailbox),
		logger:       logger,
		defaults:     defaults.orFallbacks(),
	}
}

// MailboxDefaults reports the settings applied to mailboxes created on the
// delivery path.
func (s *MailboxServer) MailboxDefaults() MailboxDefaults {
	return s.defaults
}

// getMailbox gets or creates a mailbox, using the stored type from the database.
func (s *MailboxServer) getMailbox(ctx context.Context, addr *core.MailboxAddress) (mailboxes.Mailbox, error) {
	return s.getOrCreateMailbox(ctx, addr, s.defaults.MaxMessages, s.defaults.RetentionDays, nil)
}

// getOrCreateMailbox returns the existing mailbox at addr or creates it with
// the given settings, refusing the create when it would exceed a quota.
//
// The quota check sits between find and create rather than inside the
// storage upsert, so two concurrent creates can each pass it and land one
// over the line. That is the trade: a single query on the rare create path
// against a serialising lock on every delivery to a new address.
func (s *MailboxServer) getOrCreateMailbox(ctx context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) (mailboxes.Mailbox, error) {
	key := addr.FullPath()

	s.mu.RLock()
	if mb, ok := s.mailboxCache[key]; ok {
		s.mu.RUnlock()
		return mb, nil
	}
	s.mu.RUnlock()

	existing, err := s.Storage.FindMailbox(ctx, addr.OwnerID, addr.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("find mailbox: %w", err)
	}
	if existing == nil {
		if err := s.checkMailboxQuota(ctx, addr.OwnerID); err != nil {
			return nil, err
		}
	}

	// An existing mailbox keeps the settings it was created with; these
	// apply only when this call is what brings the mailbox into existence.
	record, err := s.Storage.GetOrCreateMailbox(ctx, addr, maxMessages, retentionDays, retentionCount)
	if err != nil {
		return nil, fmt.Errorf("get or create mailbox: %w", err)
	}

	// Instantiate correct type based on stored record type
	mb, err := s.wrapRecord(record)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.mailboxCache[key] = mb
	s.mu.Unlock()

	s.logger.Debug("loaded mailbox",
		"type", record.Type,
		"path", addr.FullPath(),
	)

	return mb, nil
}

// checkMailboxQuota refuses a create that would take the owner or the
// server past its configured mailbox count.
func (s *MailboxServer) checkMailboxQuota(ctx context.Context, ownerID peer.ID) error {
	perOwner, total := s.defaults.MaxMailboxesPerOwner, s.defaults.MaxMailboxes
	if perOwner <= 0 && total <= 0 {
		return nil
	}
	owned, all, err := s.Storage.CountMailboxes(ctx, ownerID)
	if err != nil {
		return fmt.Errorf("count mailboxes: %w", err)
	}
	if perOwner > 0 && owned >= perOwner {
		return &mailboxes.QuotaExceededError{What: "mailboxes per owner", Current: owned, Max: perOwner}
	}
	if total > 0 && all >= total {
		return &mailboxes.QuotaExceededError{What: "mailboxes", Current: all, Max: total}
	}
	return nil
}

// clampExpiry bounds a message's life to its mailbox's retention. The
// sender chooses the expiry, and a sender who chose the year 2200 used to
// get exactly that; a message now lives at most retention_days from now
// whatever it asked for, and one that asked for nothing gets the same.
func clampExpiry(msg *core.Message, record *storage.MailboxRecord, now time.Time) {
	if record.RetentionDays <= 0 {
		return
	}
	limit := now.Add(time.Duration(record.RetentionDays) * 24 * time.Hour).UnixMilli()
	if msg.ExpiryTimestamp <= 0 || msg.ExpiryTimestamp > limit {
		msg.ExpiryTimestamp = limit
	}
}

// DeliverLocal delivers a message to a local mailbox.
func (s *MailboxServer) DeliverLocal(ctx context.Context, msg *core.Message) (int, error) {
	s.logger.Debug("delivering message",
		"message_id", msg.MessageID,
		"from", msg.SenderPeerID,
		"to", msg.RecipientPeerID,
		"folder", msg.FolderPath,
	)

	folderPath := msg.FolderPath
	if folderPath == "" {
		folderPath = "inbox"
	}

	recipientID, err := peer.Decode(msg.RecipientPeerID)
	if err != nil {
		return 0, fmt.Errorf("decode recipient peer ID: %w", err)
	}

	// The sender names the folder, so the path is validated here as it is
	// on an explicit create; delivery used to skip that and accept anything
	// up to a frame in length.
	addr, err := core.NewMailboxAddress(recipientID, folderPath, core.MailboxPrivate)
	if err != nil {
		return 0, &mailboxes.InvalidPathError{Path: folderPath, Reason: err}
	}

	mb, err := s.getMailbox(ctx, addr)
	if err != nil {
		return 0, err
	}

	clampExpiry(msg, mb.Record(), time.Now())

	if err := mb.StoreMessage(ctx, msg); err != nil {
		return 0, err
	}

	s.logger.Debug("delivered message",
		"message_id", msg.MessageID,
		"mailbox", addr.FullPath(),
	)

	// Fire push notification (non-blocking).
	if s.notifier != nil {
		s.notifier.NotifyNewMessage(ctx, addr, msg)
	}

	return int(msg.SequenceNumber), nil
}

// findMailbox loads a mailbox that already exists, or returns nil when it does
// not. Unlike getMailbox it never creates one.
//
// The distinction matters on the read path. Retrieval used to go through
// getMailbox with a type inferred from "caller is not the owner", which meant
// any peer could bring victim/inbox into existence as a public mailbox before
// the victim ever connected — after which the victim's own senders were
// refused for lacking an ACL grant, and anything that did land was readable
// by everyone. Only delivery and an explicit create may bring a mailbox into
// existence; a read of one that is absent is simply a miss.
func (s *MailboxServer) findMailbox(ctx context.Context, addr *core.MailboxAddress) (mailboxes.Mailbox, error) {
	key := addr.FullPath()

	s.mu.RLock()
	if mb, ok := s.mailboxCache[key]; ok {
		s.mu.RUnlock()
		return mb, nil
	}
	s.mu.RUnlock()

	record, err := s.Storage.FindMailbox(ctx, addr.OwnerID, addr.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("find mailbox: %w", err)
	}
	if record == nil {
		return nil, nil
	}

	mb, err := s.wrapRecord(record)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.mailboxCache[key] = mb
	s.mu.Unlock()

	return mb, nil
}

// wrapRecord instantiates the mailbox type the stored record says it is.
func (s *MailboxServer) wrapRecord(record *storage.MailboxRecord) (mailboxes.Mailbox, error) {
	switch record.Type {
	case core.MailboxPrivate:
		return mailboxes.NewPrivateMailbox(record, s.Storage, s.logger), nil
	case core.MailboxShared:
		return mailboxes.NewSharedMailbox(record, s.Storage, s.logger), nil
	case core.MailboxPublic:
		return mailboxes.NewPublicMailbox(record, s.Storage, s.logger), nil
	default:
		return nil, fmt.Errorf("unknown mailbox type: %d", record.Type)
	}
}

// Retrieve retrieves messages from a mailbox.
//
// The mailbox's stored type decides who may read it; addr.Type is ignored.
// A mailbox that does not exist is an empty result for its owner — a fresh
// identity reading its own inbox before anything was delivered is not an
// error — and a NotFoundError for anyone else.
func (s *MailboxServer) Retrieve(ctx context.Context, addr *core.MailboxAddress, callerID peer.ID, opts RetrieveOpts) ([]*core.Message, bool, error) {
	s.logger.Debug("retrieving messages",
		"mailbox", addr.FullPath(),
		"caller", callerID.String(),
	)

	mb, err := s.findMailbox(ctx, addr)
	if err != nil {
		return nil, false, err
	}
	if mb == nil {
		if callerID == addr.OwnerID {
			return []*core.Message{}, false, nil
		}
		return nil, false, &mailboxes.NotFoundError{Path: addr.FullPath()}
	}

	messages, hasMore, err := mb.RetrieveMessages(ctx, &callerID, mailboxes.RetrieveOpts{
		FromSequence: opts.FromSequence,
		MaxMessages:  opts.MaxMessages,
		MinPriority:  opts.MinPriority,
		MaxBytes:     opts.MaxBytes,
	})
	if err != nil {
		return nil, false, err
	}

	s.logger.Debug("retrieved messages",
		"count", len(messages),
		"mailbox", addr.FullPath(),
	)

	return messages, hasMore, nil
}

// MarkDelivered acknowledges the caller's own messages and reports how many
// were touched: non-persistent ones are removed, persistent ones gain \Seen.
// This is the one place a read-side operation removes mail, and it is
// explicit — retrieval itself never does. IDs that are not in a mailbox the
// caller owns are ignored, not refused: a message ID is sender-chosen and
// travels in every acknowledgement, so it must not be a capability.
func (s *MailboxServer) MarkDelivered(ctx context.Context, callerID peer.ID, messageIDs []string) (int, error) {
	return s.Storage.MarkMessagesDelivered(ctx, callerID, messageIDs)
}

// UpdateFlags applies IMAP-style flag changes to one of the caller's own
// messages and returns the resulting flags, or nil when the caller owns no
// message with that ID.
func (s *MailboxServer) UpdateFlags(ctx context.Context, callerID peer.ID, messageID string, addFlags, removeFlags uint32) (*uint32, error) {
	return s.Storage.UpdateMessageFlags(ctx, callerID, messageID, addFlags, removeFlags)
}

// DeleteMessages removes the caller's own messages by ID and reports how many
// existed. See MarkDelivered for why foreign IDs are silently unmatched.
func (s *MailboxServer) DeleteMessages(ctx context.Context, callerID peer.ID, messageIDs []string) (int, error) {
	return s.Storage.DeleteOwnedMessages(ctx, callerID, messageIDs)
}

// CreateMailbox creates a new mailbox with the given options, subject to
// the same quotas as a delivery-created one.
func (s *MailboxServer) CreateMailbox(ctx context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) error {
	if _, err := s.getOrCreateMailbox(ctx, addr, maxMessages, retentionDays, retentionCount); err != nil {
		return err
	}

	s.logger.Debug("created mailbox",
		"type", addr.Type,
		"path", addr.FullPath(),
	)
	return nil
}

// DeleteMailbox deletes a mailbox.
func (s *MailboxServer) DeleteMailbox(ctx context.Context, addr *core.MailboxAddress) error {
	record, err := s.Storage.FindMailbox(ctx, addr.OwnerID, addr.FolderPath)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("mailbox not found: %s", addr.FullPath())
	}

	if err := s.Storage.DeleteMailbox(ctx, record.ID); err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.mailboxCache, addr.FullPath())
	s.mu.Unlock()

	if s.notifier != nil {
		s.notifier.MailboxDeleted(addr)
	}

	s.logger.Debug("deleted mailbox", "path", addr.FullPath())
	return nil
}

// ListMailboxes lists all mailboxes for a peer.
func (s *MailboxServer) ListMailboxes(ctx context.Context, ownerID peer.ID) ([]*storage.MailboxRecord, error) {
	return s.Storage.ListMailboxes(ctx, ownerID)
}

// PerformMaintenance runs cleanup tasks.
func (s *MailboxServer) PerformMaintenance(ctx context.Context) error {
	s.logger.Info("starting MDA maintenance")

	expiredCount, err := s.Storage.DeleteExpiredMessages(ctx)
	if err != nil {
		return err
	}
	if expiredCount > 0 {
		s.logger.Info("deleted expired messages", "count", expiredCount)
	}

	// Retention for every mailbox, private ones included. This used to
	// walk the delivery cache and act only on the public mailboxes in it,
	// so retention_days on a private mailbox was decoration.
	retained, err := s.Storage.EnforceAllRetention(ctx)
	if err != nil {
		s.logger.Warn("failed to enforce mailbox retention", "error", err)
	} else if retained > 0 {
		s.logger.Info("retention removed messages", "count", retained)
	}

	// The counters the cap check reads are maintained by the store and
	// delete paths; this is the safety net, and drift means a bug.
	drifted, err := s.Storage.ReconcileMessageCounts(ctx)
	if err != nil {
		s.logger.Warn("failed to reconcile message counts", "error", err)
	} else if drifted > 0 {
		s.logger.Warn("message counts had drifted", "mailboxes", drifted)
	}

	feedRetained, err := s.Storage.EnforceAllFeedRetention(ctx)
	if err != nil {
		s.logger.Warn("failed to enforce feed retention", "error", err)
	} else if feedRetained > 0 {
		s.logger.Info("feed retention removed entries", "count", feedRetained)
	}

	s.logger.Info("MDA maintenance complete")
	return nil
}

// Close closes the MDA.
func (s *MailboxServer) Close() error {
	s.logger.Info("closing MDA")
	s.mu.Lock()
	s.mailboxCache = make(map[string]mailboxes.Mailbox)
	s.mu.Unlock()
	return s.Storage.Close()
}
