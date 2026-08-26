package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/protocol/frame"
	"github.com/twostack/go-ricochet/internal/protocol/sfa"
)

// FeedInfo contains metadata about a feed.
type FeedInfo struct {
	Path            string `json:"path"`
	Title           string `json:"title"`
	Description     string `json:"description"`
	CurrentSequence int    `json:"currentSequence"`
	LastEntryAt     int64  `json:"lastEntryAt"`
	CreatedAt       int64  `json:"createdAt"`
}

// FeedEntry represents a single entry in a feed.
type FeedEntry struct {
	Seq       int    `json:"seq"`
	Type      string `json:"type,omitempty"`
	Content   []byte `json:"content"`
	Hash      string `json:"hash"`
	CreatedAt int64  `json:"createdAt"`
	CreatedBy string `json:"createdBy,omitempty"`
}

// ---------------------------------------------------------------------------
// SFA operations
// ---------------------------------------------------------------------------

// doFeed is the shared helper for all SFA feed operations.
func (c *Client) doFeed(ctx context.Context, req *sfa.FeedRequest, opts []FeedOption) (*sfa.FeedResponse, error) {
	cfg := feedConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal feed request: %w", err)
	}

	var serverID peer.ID
	if cfg.ServerPeerID != nil {
		serverID = *cfg.ServerPeerID
	} else {
		serverID, err = c.selectServer()
		if err != nil {
			return nil, err
		}
	}

	s, err := c.openStream(ctx, serverID, sfa.ProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write feed frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read feed response: %w", err)
	}

	var resp sfa.FeedResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal feed response: %w", err)
	}

	return &resp, nil
}

// CreateFeed creates a new feed on the server.
func (c *Client) CreateFeed(ctx context.Context, path, title, description string, opts ...FeedOption) error {
	cfg := feedConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	req := &sfa.FeedRequest{
		Operation:     sfa.OpCREATE,
		OwnerPeerID:   c.host.ID().String(),
		Path:          path,
		Title:         title,
		Description:   description,
		Collaborative: cfg.Collaborative,
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return err
	}

	if resp.Status >= 400 {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("create feed failed with status %d", resp.Status)
		}
		return fmt.Errorf("create feed: %s", errMsg)
	}

	return nil
}

// AppendFeedEntry appends an entry to a feed. Returns the sequence number.
func (c *Client) AppendFeedEntry(ctx context.Context, path string, content []byte, entryType string, opts ...FeedOption) (int, error) {
	req := &sfa.FeedRequest{
		Operation:   sfa.OpAPPEND,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
		EntryType:   entryType,
		Body:        base64.StdEncoding.EncodeToString(content),
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return 0, err
	}

	if resp.Status >= 400 {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("append feed entry failed with status %d", resp.Status)
		}
		return 0, fmt.Errorf("append feed entry: %s", errMsg)
	}

	seq := int(headerToInt64(resp.Headers["X-Sequence"]))
	return seq, nil
}

// AppendToFeed appends an entry to another peer's collaborative feed. Returns the sequence number.
func (c *Client) AppendToFeed(ctx context.Context, ownerPeerID peer.ID, path string, content []byte, entryType string, opts ...FeedOption) (int, error) {
	req := &sfa.FeedRequest{
		Operation:   sfa.OpAPPEND,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
		EntryType:   entryType,
		Body:        base64.StdEncoding.EncodeToString(content),
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return 0, err
	}

	if resp.Status >= 400 {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("append to feed failed with status %d", resp.Status)
		}
		return 0, fmt.Errorf("append to feed: %s", errMsg)
	}

	seq := int(headerToInt64(resp.Headers["X-Sequence"]))
	return seq, nil
}

// GetFeed retrieves feed metadata.
func (c *Client) GetFeed(ctx context.Context, ownerPeerID peer.ID, path string, opts ...FeedOption) (*FeedInfo, error) {
	req := &sfa.FeedRequest{
		Operation:   sfa.OpGET,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status == sfa.StatusNotFound {
		return nil, nil
	}

	if resp.Status != sfa.StatusOK {
		return nil, fmt.Errorf("get feed failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode feed body: %w", err)
	}

	var info FeedInfo
	info.Path = path
	var raw struct {
		Title           string `json:"title"`
		Description     string `json:"description"`
		CurrentSequence int    `json:"currentSequence"`
		LastEntryAt     int64  `json:"lastEntryAt"`
		CreatedAt       int64  `json:"createdAt"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal feed info: %w", err)
	}
	info.Title = raw.Title
	info.Description = raw.Description
	info.CurrentSequence = raw.CurrentSequence
	info.LastEntryAt = raw.LastEntryAt
	info.CreatedAt = raw.CreatedAt

	return &info, nil
}

// GetFeedEntry retrieves a single feed entry by sequence number.
func (c *Client) GetFeedEntry(ctx context.Context, ownerPeerID peer.ID, path string, seq int, opts ...FeedOption) (*FeedEntry, error) {
	req := &sfa.FeedRequest{
		Operation:      sfa.OpGET,
		OwnerPeerID:    ownerPeerID.String(),
		Path:           path,
		SequenceNumber: &seq,
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status == sfa.StatusNotFound {
		return nil, nil
	}

	if resp.Status != sfa.StatusOK {
		return nil, fmt.Errorf("get feed entry failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, nil
	}

	content, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode feed entry body: %w", err)
	}

	entry := &FeedEntry{
		Seq:     seq,
		Content: content,
	}
	if etag, ok := resp.Headers["ETag"]; ok {
		entry.Hash, _ = etag.(string)
	}
	if et, ok := resp.Headers["X-Entry-Type"]; ok {
		entry.Type, _ = et.(string)
	}
	if ca, ok := resp.Headers["Created-At"]; ok {
		entry.CreatedAt = headerToInt64(ca)
	}

	return entry, nil
}

// GetFeedEntries retrieves a range of feed entries.
func (c *Client) GetFeedEntries(ctx context.Context, ownerPeerID peer.ID, path string, opts ...FeedEntryOption) ([]*FeedEntry, bool, error) {
	cfg := feedEntryConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	req := &sfa.FeedRequest{
		Operation:    sfa.OpGET,
		OwnerPeerID:  ownerPeerID.String(),
		Path:         path,
		FromSequence: cfg.FromSequence,
		ToSequence:   cfg.ToSequence,
		Limit:        cfg.Limit,
		EntryType:    cfg.EntryType,
	}

	var feedOpts []FeedOption
	if cfg.ServerPeerID != nil {
		feedOpts = append(feedOpts, WithFeedServer(*cfg.ServerPeerID))
	}

	resp, err := c.doFeed(ctx, req, feedOpts)
	if err != nil {
		return nil, false, err
	}

	if resp.Status == sfa.StatusNotFound {
		return nil, false, fmt.Errorf("feed not found")
	}

	if resp.Status != sfa.StatusOK {
		return nil, false, fmt.Errorf("get feed entries failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, false, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("decode entries body: %w", err)
	}

	var raw struct {
		Entries []struct {
			Seq       int    `json:"seq"`
			Type      string `json:"type"`
			Content   string `json:"content"` // base64
			Hash      string `json:"hash"`
			CreatedAt int64  `json:"createdAt"`
			CreatedBy string `json:"createdBy"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, false, fmt.Errorf("unmarshal entries: %w", err)
	}

	entries := make([]*FeedEntry, 0, len(raw.Entries))
	for _, e := range raw.Entries {
		content, err := base64.StdEncoding.DecodeString(e.Content)
		if err != nil {
			return nil, false, fmt.Errorf("decode entry content: %w", err)
		}
		entries = append(entries, &FeedEntry{
			Seq:       e.Seq,
			Type:      e.Type,
			Content:   content,
			Hash:      e.Hash,
			CreatedAt: e.CreatedAt,
			CreatedBy: e.CreatedBy,
		})
	}

	hasMore := false
	if hm, ok := resp.Headers["X-Has-More"]; ok {
		hasMore, _ = hm.(bool)
	}

	return entries, hasMore, nil
}

// BatchFeedQuery describes a single feed to retrieve in a batch request.
type BatchFeedQuery struct {
	OwnerPeerID  peer.ID
	Path         string
	FromSequence *int
	Limit        *int
}

// BatchFeedResult holds the entries returned for one feed in a batch request.
type BatchFeedResult struct {
	Entries []*FeedEntry
	HasMore bool
	Error   string
}

// GetMultiFeedEntries retrieves entries from multiple feeds in a single request.
// Results are keyed by "ownerPeerID/path".
func (c *Client) GetMultiFeedEntries(ctx context.Context, queries []BatchFeedQuery, opts ...FeedOption) (map[string]*BatchFeedResult, error) {
	batchQueries := make([]sfa.BatchQuery, 0, len(queries))
	for _, q := range queries {
		batchQueries = append(batchQueries, sfa.BatchQuery{
			OwnerPeerID:  q.OwnerPeerID.String(),
			Path:         q.Path,
			FromSequence: q.FromSequence,
			Limit:        q.Limit,
		})
	}

	req := &sfa.FeedRequest{
		Operation:    sfa.OpBATCH_GET,
		BatchQueries: batchQueries,
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sfa.StatusOK {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("batch get failed with status %d", resp.Status)
		}
		return nil, fmt.Errorf("batch get feed entries: %s", errMsg)
	}

	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode batch body: %w", err)
	}

	var raw struct {
		Feeds map[string]struct {
			Entries []struct {
				Seq       int    `json:"seq"`
				Type      string `json:"type"`
				Content   string `json:"content"`
				Hash      string `json:"hash"`
				CreatedAt int64  `json:"createdAt"`
				CreatedBy string `json:"createdBy"`
			} `json:"entries"`
			HasMore bool   `json:"hasMore"`
			Error   string `json:"error"`
		} `json:"feeds"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal batch response: %w", err)
	}

	results := make(map[string]*BatchFeedResult, len(raw.Feeds))
	for key, fr := range raw.Feeds {
		result := &BatchFeedResult{
			HasMore: fr.HasMore,
			Error:   fr.Error,
			Entries: make([]*FeedEntry, 0, len(fr.Entries)),
		}
		for _, e := range fr.Entries {
			content, err := base64.StdEncoding.DecodeString(e.Content)
			if err != nil {
				return nil, fmt.Errorf("decode entry content in %s: %w", key, err)
			}
			result.Entries = append(result.Entries, &FeedEntry{
				Seq:       e.Seq,
				Type:      e.Type,
				Content:   content,
				Hash:      e.Hash,
				CreatedAt: e.CreatedAt,
				CreatedBy: e.CreatedBy,
			})
		}
		results[key] = result
	}

	return results, nil
}

// DeleteFeed deletes a feed. Returns true if the feed existed and was deleted.
func (c *Client) DeleteFeed(ctx context.Context, path string, opts ...FeedOption) (bool, error) {
	req := &sfa.FeedRequest{
		Operation:   sfa.OpDELETE,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return false, err
	}

	return resp.Status != sfa.StatusNotFound, nil
}

// ListFeeds lists all feeds for the given owner.
func (c *Client) ListFeeds(ctx context.Context, ownerPeerID peer.ID, opts ...FeedOption) ([]FeedInfo, error) {
	req := &sfa.FeedRequest{
		Operation:   sfa.OpLIST,
		OwnerPeerID: ownerPeerID.String(),
	}

	resp, err := c.doFeed(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sfa.StatusOK {
		return nil, fmt.Errorf("list feeds failed with status %d", resp.Status)
	}

	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode list body: %w", err)
	}

	var infos []FeedInfo
	if err := json.Unmarshal(bodyBytes, &infos); err != nil {
		return nil, fmt.Errorf("unmarshal feed list: %w", err)
	}

	return infos, nil
}
