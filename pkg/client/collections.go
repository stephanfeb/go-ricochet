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
	Items      []*CollectionItem `json:"items"`
	TotalCount int               `json:"totalCount"`
	HasMore    bool              `json:"hasMore"`
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

	var serverID peer.ID
	if cfg.ServerPeerID != nil {
		serverID = *cfg.ServerPeerID
	} else {
		serverID, err = c.selectServer()
		if err != nil {
			return nil, err
		}
	}

	s, err := c.openStream(ctx, serverID, sca.ProtocolID)
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

	// Convert to CollectionOption for the shared doCollection helper
	var collOpts []CollectionOption
	if cfg.ServerPeerID != nil {
		collOpts = append(collOpts, WithCollectionServer(*cfg.ServerPeerID))
	}

	return c.doCollection(ctx, req, collOpts)
}

// CreateCollection creates a new collection on the server.
func (c *Client) CreateCollection(ctx context.Context, path, name string, opts ...CollectionOption) error {
	req := &sca.CollectionRequest{
		Operation:   sca.OpCREATE,
		OwnerPeerID: c.host.ID().String(),
		Path:        path,
		Name:        name,
	}

	resp, err := c.doCollection(ctx, req, opts)
	if err != nil {
		return err
	}

	if resp.Status >= 400 {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("create collection failed with status %d", resp.Status)
		}
		return fmt.Errorf("create collection: %s", errMsg)
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
		return nil, fmt.Errorf("get collection failed with status %d", resp.Status)
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
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal collection info: %w", err)
	}
	info.Name = raw.Name
	info.RecordCount = raw.RecordCount
	info.LastModifiedAt = raw.LastModifiedAt
	info.CreatedAt = raw.CreatedAt

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

	return resp.Status != sca.StatusNotFound, nil
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
		return nil, fmt.Errorf("list collections failed with status %d", resp.Status)
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

	if resp.Status == sca.StatusConflict {
		errMsg, _ := resp.Headers["Error"].(string)
		return nil, fmt.Errorf("collection item conflict: %s", errMsg)
	}

	if resp.Status >= 400 {
		errMsg, _ := resp.Headers["Error"].(string)
		if errMsg == "" {
			errMsg = fmt.Sprintf("put collection item failed with status %d", resp.Status)
		}
		return nil, fmt.Errorf("put collection item: %s", errMsg)
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
		return nil, fmt.Errorf("get collection item failed with status %d", resp.Status)
	}
	if resp.Body == "" {
		return nil, nil
	}

	content, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode collection item body: %w", err)
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

	return resp.Status != sca.StatusNotFound, nil
}

// ListCollectionKeys returns all keys in a collection.
func (c *Client) ListCollectionKeys(ctx context.Context, ownerPeerID peer.ID, path string, opts ...CollectionQueryOption) ([]string, int, error) {
	req := &sca.CollectionRequest{
		Operation:   sca.OpLIST,
		OwnerPeerID: ownerPeerID.String(),
		Path:        path,
	}

	resp, err := c.doCollectionQuery(ctx, req, opts)
	if err != nil {
		return nil, 0, err
	}

	if resp.Status == sca.StatusNotFound {
		return nil, 0, fmt.Errorf("collection not found")
	}
	if resp.Status != sca.StatusOK {
		return nil, 0, fmt.Errorf("list collection keys failed with status %d", resp.Status)
	}
	if resp.Body == "" {
		return nil, 0, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("decode keys body: %w", err)
	}

	var raw struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return nil, 0, fmt.Errorf("unmarshal keys: %w", err)
	}

	totalCount := 0
	if tc, ok := resp.Headers["X-Total-Count"]; ok {
		totalCount = int(headerToInt64(tc))
	}

	return raw.Keys, totalCount, nil
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

	if resp.Status == sca.StatusNotFound {
		return nil, fmt.Errorf("collection not found")
	}
	if resp.Status != sca.StatusOK {
		errMsg, _ := resp.Headers["Error"].(string)
		return nil, fmt.Errorf("query collection failed with status %d: %s", resp.Status, errMsg)
	}
	if resp.Body == "" {
		return &CollectionQueryResponse{}, nil
	}

	bodyBytes, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode query body: %w", err)
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
		Items: items,
	}
	if tc, ok := resp.Headers["X-Total-Count"]; ok {
		result.TotalCount = int(headerToInt64(tc))
	}
	if hm, ok := resp.Headers["X-Has-More"]; ok {
		result.HasMore, _ = hm.(bool)
	}

	return result, nil
}
