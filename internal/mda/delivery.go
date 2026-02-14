package mda

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

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
}

// MailboxServer is the Mail Delivery Agent — handles local message storage and retrieval.
type MailboxServer struct {
	Storage      storage.Storage
	mailboxCache map[string]mailboxes.Mailbox
	mu           sync.RWMutex
	logger       *slog.Logger
}

// NewMailboxServer creates a new MDA.
func NewMailboxServer(store storage.Storage, logger *slog.Logger) *MailboxServer {
	return &MailboxServer{
		Storage:      store,
		mailboxCache: make(map[string]mailboxes.Mailbox),
		logger:       logger,
	}
}

// getMailbox gets or creates a mailbox, using the stored type from the database.
func (s *MailboxServer) getMailbox(ctx context.Context, addr *core.MailboxAddress) (mailboxes.Mailbox, error) {
	key := addr.FullPath()

	s.mu.RLock()
	if mb, ok := s.mailboxCache[key]; ok {
		s.mu.RUnlock()
		return mb, nil
	}
	s.mu.RUnlock()

	// Get or create mailbox in storage
	record, err := s.Storage.GetOrCreateMailbox(ctx, addr, 1000, 30, nil)
	if err != nil {
		return nil, fmt.Errorf("get or create mailbox: %w", err)
	}

	// Instantiate correct type based on stored record type
	var mb mailboxes.Mailbox
	switch record.Type {
	case core.MailboxPrivate:
		mb = mailboxes.NewPrivateMailbox(record, s.Storage, s.logger)
	case core.MailboxShared:
		mb = mailboxes.NewSharedMailbox(record, s.Storage, s.logger)
	case core.MailboxPublic:
		mb = mailboxes.NewPublicMailbox(record, s.Storage, s.logger)
	default:
		return nil, fmt.Errorf("unknown mailbox type: %d", record.Type)
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

// DeliverLocal delivers a message to a local mailbox.
func (s *MailboxServer) DeliverLocal(ctx context.Context, msg *core.Message) (int, error) {
	s.logger.Info("delivering message",
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

	addr := &core.MailboxAddress{
		OwnerID:    recipientID,
		FolderPath: folderPath,
		Type:       core.MailboxPrivate, // Default for auto-created mailboxes
	}

	mb, err := s.getMailbox(ctx, addr)
	if err != nil {
		return 0, err
	}

	if err := mb.StoreMessage(ctx, msg); err != nil {
		return 0, err
	}

	s.logger.Info("delivered message",
		"message_id", msg.MessageID,
		"mailbox", addr.FullPath(),
	)

	return int(msg.SequenceNumber), nil
}

// Retrieve retrieves messages from a mailbox.
func (s *MailboxServer) Retrieve(ctx context.Context, addr *core.MailboxAddress, callerID peer.ID, opts RetrieveOpts) ([]*core.Message, error) {
	s.logger.Info("retrieving messages",
		"mailbox", addr.FullPath(),
		"caller", callerID.String(),
	)

	mb, err := s.getMailbox(ctx, addr)
	if err != nil {
		return nil, err
	}

	messages, err := mb.RetrieveMessages(ctx, &callerID, mailboxes.RetrieveOpts{
		FromSequence: opts.FromSequence,
		MaxMessages:  opts.MaxMessages,
		MinPriority:  opts.MinPriority,
	})
	if err != nil {
		return nil, err
	}

	s.logger.Info("retrieved messages",
		"count", len(messages),
		"mailbox", addr.FullPath(),
	)

	return messages, nil
}

// CreateMailbox creates a new mailbox with the given options.
func (s *MailboxServer) CreateMailbox(ctx context.Context, addr *core.MailboxAddress, maxMessages, retentionDays int, retentionCount *int) error {
	_, err := s.Storage.GetOrCreateMailbox(ctx, addr, maxMessages, retentionDays, retentionCount)
	if err != nil {
		return fmt.Errorf("create mailbox: %w", err)
	}

	s.logger.Info("created mailbox",
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

	s.logger.Info("deleted mailbox", "path", addr.FullPath())
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

	// Enforce retention on public mailboxes
	s.mu.RLock()
	var publicMailboxes []*storage.MailboxRecord
	for _, mb := range s.mailboxCache {
		if mb.Record().Type == core.MailboxPublic {
			publicMailboxes = append(publicMailboxes, mb.Record())
		}
	}
	s.mu.RUnlock()

	for _, record := range publicMailboxes {
		if err := s.Storage.EnforceRetentionPolicy(ctx, record); err != nil {
			s.logger.Warn("failed to enforce retention policy",
				"mailbox", record.FullPath(),
				"error", err,
			)
		}
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
