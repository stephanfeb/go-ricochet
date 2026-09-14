package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/protocol/frame"
	"github.com/twostack/go-ricochet/internal/protocol/sca"
)

// CollectionInfo contains metadata about a collection.
type CollectionInfo struct {
	Path           string `json:"path"`
	Name           string `json:"name"`
	RecordCount    int    `json:"recordCount"`
	LastModifiedAt int64  `json:"lastModifiedAt"`
	CreatedAt      int64  `json:"createdAt"`
	Visibility     string `json:"visibility"` // private, shared or public
}

// CollectionItem represents a single item in a collection.
type CollectionItem struct {
	Key       string          `json:"key"`
	Content   json.RawMessage `json:"content"`
	Hash      string          `json:"hash"`
	Version   int             `json:"version"`
	UpdatedAt int64           `json:"updatedAt"`
}

// CollectionItemResult is the result of a put operation.
type CollectionItemResult struct {
	Created bool
	ETag    string
	Version int
}

// CollectionQueryResponse wraps a paged query result.
type CollectionQueryResponse struct {
	Items []*CollectionItem `json:"items"`
	// TotalCount is -1 on a page fetched with WithQueryCursor.
	TotalCount int  `json:"totalCount"`
	HasMore    bool `json:"hasMore"`
	// NextCursor fetches the page after this one; empty on the last.
	NextCursor string `json:"nextCursor,omitempty"`
}

// CollectionKeysPage is one page of keys.
type CollectionKeysPage struct {
	Keys []string `json:"keys"`
	// TotalCount is -1 on a page fetched with WithQueryCursor.
	TotalCount int    `json:"totalCount"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// ---------------------------------------------------------------------------
// SCA operations
// ---------------------------------------------------------------------------

// doCollection is the shared helper for all SCA collection operations.
func (c *Client) doCollection(ctx context.Context, req *sca.CollectionRequest, opts []CollectionOption) (*sca.CollectionResponse, error) {
	cfg := collectionConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal collection request: %w", err)
	}

	s, _, err := c.dial(ctx, sca.ProtocolID, cfg.ServerPeerID)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	if err := frame.WriteFrame(s, reqData); err != nil {
		return nil, fmt.Errorf("write collection frame: %w", err)
	}

	respData, err := frame.ReadFrame(s)
	if err != nil {
		return nil, fmt.Errorf("read collection response: %w", err)
	}

	var resp sca.CollectionResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal collection response: %w", err)
	}

	return &resp, nil
}

// doCollectionQuery is the shared helper for query operations that use CollectionQueryOption.
func (c *Client) doCollectionQuery(ctx context.Context, req *sca.CollectionRequest, opts []CollectionQueryOption) (*sca.CollectionResponse, error) {
	cfg := collectionQueryConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	// Apply query config to request
	if cfg.SortField != "" {
		req.SortField = cfg.SortField
	}
	if cfg.SortAsc != nil {
		req.SortAsc = cfg.SortAsc
	}
	if cfg.Limit != nil {
		req.Limit = cfg.Limit
	}
	if cfg.Offset != nil {
		req.Offset = cfg.Offset
	}
	if cfg.Cursor != "" {
		req.Cursor = cfg.Cursor
	}

	// Convert to CollectionOption for the shared doCollection helper
	var collOpts []CollectionOption
	if cfg.ServerPeerID != nil {
		collOpts = append(collOpts, WithCollectionServer(*cfg.ServerPeerID))
	}

	return c.doCollection(ctx, req, collOpts)
}

// CreateCollection creates a new collection on the server.
func (c *Client) CreateCollection(ctx context.Context, path, name string, opts ...CollectionOption) error {
	cfg := collectionConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	req := &sca.CollectionRequest{
		Operation:   sca.OpCREATE,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
		Name:        name,
	}
	if cfg.Visibility != nil {
		req.Visibility = cfg.Visibility.String()
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return err
	}

	if resp.Status >= 400 {
		return responseError("create collection", resp.Status, resp.Headers)
	}

	return nil
}

// GetCollection retrieves collection metadata.
func (c *Client) GetCollection(ctx context.Context, ownerPeerID peer.ID, path string, opts ...CollectionOption) (*CollectionInfo, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpGET,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status == sca.StatusNotFound {
		return nil, nil
	}
	if resp.Status != sca.StatusOK {
		return nil, responseError("get collection", resp.Status, resp.Headers)
	}
	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode collection body: %w", err)
	}

	var info CollectionInfo
	info.Path = path
	var raw struct {
		Name           string `json:"name"`
		RecordCount    int    `json:"recordCount"`
		LastModifiedAt int64  `json:"lastModifiedAt"`
		CreatedAt      int64  `json:"createdAt"`
		Visibility     string `json:"visibility"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal collection info: %w", err)
	}
	info.Name = raw.Name
	info.RecordCount = raw.RecordCount
	info.LastModifiedAt = raw.LastModifiedAt
	info.CreatedAt = raw.CreatedAt
	info.Visibility = raw.Visibility

	return &info, nil
}

// DeleteCollection deletes a collection. Returns true if the collection existed and was deleted.
func (c *Client) DeleteCollection(ctx context.Context, path string, opts ...CollectionOption) (bool, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpDELETE,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return false, err
	}

	return deleteOutcome("collection delete", resp.Status, resp.Headers)
}

// ListCollections lists all collections for the given owner.
func (c *Client) ListCollections(ctx context.Context, ownerPeerID peer.ID, opts ...CollectionOption) ([]CollectionInfo, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpLIST,
		OwnerPeerID: ownerPeerID.String(),
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sca.StatusOK {
		return nil, responseError("list collections", resp.Status, resp.Headers)
	}
	if resp.Body == "" {
		return nil, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode list body: %w", err)
	}

	var infos []CollectionInfo
	if err := json.Unmarshal(bodyBytes, &infos); err != nil {
		return nil, fmt.Errorf("unmarshal collection list: %w", err)
	}

	return infos, nil
}

// PutCollectionItem upserts a record in a collection.
func (c *Client) PutCollectionItem(ctx context.Context, path, key string, content []byte, opts ...CollectionOption) (*CollectionItemResult, error) {
	cfg := collectionConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	req := &sca.CollectionRequest{
		Operation:   sca.OpPUT,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
		Key:         key,
		Body:        base64.StdEncoding.EncodeToString(content),
	}

	if cfg.IfMatch != "" {
		req.Headers = map[string]string{"If-Match": cfg.IfMatch}
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status >= 400 {
		return nil, responseError("put collection item", resp.Status, resp.Headers)
	}

	result := &CollectionItemResult{
		Created: resp.Status == sca.StatusCreated,
	}
	if etag, ok := resp.Headers["ETag"]; ok {
		result.ETag, _ = etag.(string)
	}
	if ver, ok := resp.Headers["X-Version"]; ok {
		result.Version = int(headerToInt64(ver))
	}

	return result, nil
}

// GetCollectionItem retrieves a single collection item by key.
func (c *Client) GetCollectionItem(ctx context.Context, ownerPeerID peer.ID, path, key string, opts ...CollectionOption) (*CollectionItem, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpGET,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
		Key:         key,
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status == sca.StatusNotFound {
		return nil, nil
	}
	if resp.Status != sca.StatusOK {
		return nil, responseError("get collection item", resp.Status, resp.Headers)
	}
	var content []byte
	switch {
	case len(resp.Data) > 0:
		content = resp.Data
	case resp.Body != "":
		content, err = base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("decode collection item body: %w", err)
		}
	default:
		return nil, nil
	}

	item := &CollectionItem{
		Key:     key,
		Content: json.RawMessage(content),
	}
	if etag, ok := resp.Headers["ETag"]; ok {
		item.Hash, _ = etag.(string)
	}
	if ver, ok := resp.Headers["X-Version"]; ok {
		item.Version = int(headerToInt64(ver))
	}
	if ua, ok := resp.Headers["Updated-At"]; ok {
		item.UpdatedAt = headerToInt64(ua)
	}

	return item, nil
}

// DeleteCollectionItem deletes a single item from a collection.
func (c *Client) DeleteCollectionItem(ctx context.Context, path, key string, opts ...CollectionOption) (bool, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpDELETE,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
		Key:         key,
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return false, err
	}

	return deleteOutcome("collection item delete", resp.Status, resp.Headers)
}

// ListCollectionKeys returns one page of keys and the total. Use
// ListCollectionKeysPage to page with a cursor.
func (c *Client) ListCollectionKeys(ctx context.Context, ownerPeerID peer.ID, path string, opts ...CollectionQueryOption) ([]string, int, error) {
	page, err := c.ListCollectionKeysPage(ctx, ownerPeerID, path, opts...)
	if err != nil {
		return nil, 0, err
	}
	return page.Keys, page.TotalCount, nil
}

// ListCollectionKeysPage returns one page of keys in key order, with the
// cursor that fetches the next.
func (c *Client) ListCollectionKeysPage(ctx context.Context, ownerPeerID peer.ID, path string, opts ...CollectionQueryOption) (*CollectionKeysPage, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpLIST,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doCollectionQuery(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sca.StatusOK {
		return nil, responseError("list collection keys", resp.Status, resp.Headers)
	}
	page := &CollectionKeysPage{TotalCount: -1}
	if resp.Body == "" {
		return page, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode keys body: %w", err)
	}

	var raw struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal keys: %w", err)
	}
	page.Keys = raw.Keys
	if tc, ok := resp.Headers["X-Total-Count"]; ok {
		page.TotalCount = int(headerToInt64(tc))
	}
	if hm, ok := resp.Headers["X-Has-More"]; ok {
		page.HasMore, _ = hm.(bool)
	}
	page.NextCursor, _ = resp.Headers["Next-Cursor"].(string)
	return page, nil
}

// QueryCollection queries collection items using JSONB filters.
func (c *Client) QueryCollection(ctx context.Context, ownerPeerID peer.ID, path string, filter map[string]any, opts ...CollectionQueryOption) (*CollectionQueryResponse, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpQUERY,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
		Filter:      filter,
	}

	resp, err := c.doCollectionQuery(ctx, req, opts)
	if err != nil {
		return nil, err
	}

	if resp.Status != sca.StatusOK {
		return nil, responseError("query collection", resp.Status, resp.Headers)
	}
	// The server sends the page as raw JSON in Data; older servers base64
	// it into Body. Read whichever is present.
	var bodyBytes []byte
	switch {
	case len(resp.Data) > 0:
		bodyBytes = resp.Data
	case resp.Body != "":
		bodyBytes, err = base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("decode query body: %w", err)
		}
	default:
		return &CollectionQueryResponse{}, nil
	}

	var raw struct {
		Items []struct {
			Key       string          `json:"key"`
			Content   json.RawMessage `json:"content"`
			Hash      string          `json:"hash"`
			Version   int             `json:"version"`
			UpdatedAt int64           `json:"updatedAt"`
		} `json:"items"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal query result: %w", err)
	}

	items := make([]*CollectionItem, 0, len(raw.Items))
	for _, item := range raw.Items {
		items = append(items, &CollectionItem{
			Key:       item.Key,
			Content:   item.Content,
			Hash:      item.Hash,
			Version:   item.Version,
			UpdatedAt: item.UpdatedAt,
		})
	}

	result := &CollectionQueryResponse{
		Items:      items,
		TotalCount: -1,
	}
	if tc, ok := resp.Headers["X-Total-Count"]; ok {
		result.TotalCount = int(headerToInt64(tc))
	}
	if hm, ok := resp.Headers["X-Has-More"]; ok {
		result.HasMore, _ = hm.(bool)
	}
	result.NextCursor, _ = resp.Headers["Next-Cursor"].(string)

	return result, nil
}
