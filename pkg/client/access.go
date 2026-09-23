package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/stephanfeb/go-ricochet/internal/protocol/sca"
	"github.com/stephanfeb/go-ricochet/internal/protocol/sda"
	"github.com/stephanfeb/go-ricochet/internal/protocol/sfa"
	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

// Who may read a document, feed or collection: the ACCESS operation of each
// store protocol, for the owner. Every call answers with the resource's
// visibility and reader list as they stand afterwards.

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

// DocumentAccess reports a document's visibility and reader list.
func (c *Client) DocumentAccess(ctx context.Context, path string, opts ...DocOption) (*wire.StoreAccess, error) {
	return c.documentAccess(ctx, path, sda.DocRequest{AccessAction: "get"}, opts)
}

// SetDocumentVisibility makes a document private, shared or public.
func (c *Client) SetDocumentVisibility(ctx context.Context, path string, v wire.Visibility, opts ...DocOption) (*wire.StoreAccess, error) {
	return c.documentAccess(ctx, path, sda.DocRequest{AccessAction: "set", Visibility: v.String()}, opts)
}

// GrantDocumentReader adds a peer to a document's reader list. The list
// applies while the document is shared.
func (c *Client) GrantDocumentReader(ctx context.Context, path string, reader peer.ID, opts ...DocOption) (*wire.StoreAccess, error) {
	return c.documentAccess(ctx, path, sda.DocRequest{AccessAction: "grant", ReaderPeerID: reader.String()}, opts)
}

// RevokeDocumentReader removes a peer from a document's reader list.
func (c *Client) RevokeDocumentReader(ctx context.Context, path string, reader peer.ID, opts ...DocOption) (*wire.StoreAccess, error) {
	return c.documentAccess(ctx, path, sda.DocRequest{AccessAction: "revoke", ReaderPeerID: reader.String()}, opts)
}

func (c *Client) documentAccess(ctx context.Context, path string, req sda.DocRequest, opts []DocOption) (*wire.StoreAccess, error) {
	req.Operation = sda.OpACCESS
	req.OwnerPeerID = c.host.ID().String()
	req.Path = path
	resp, err := c.doDoc(ctx, &req, opts)
	if err != nil {
		return nil, err
	}
	return decodeAccess("document access", resp.Status, resp.Headers, resp.Body)
}

// ---------------------------------------------------------------------------
// Feeds
// ---------------------------------------------------------------------------

// FeedAccess reports a feed's visibility and reader list.
func (c *Client) FeedAccess(ctx context.Context, path string, opts ...FeedOption) (*wire.StoreAccess, error) {
	return c.feedAccess(ctx, path, sfa.FeedRequest{AccessAction: "get"}, opts)
}

// SetFeedVisibility makes a feed private, shared or public.
func (c *Client) SetFeedVisibility(ctx context.Context, path string, v wire.Visibility, opts ...FeedOption) (*wire.StoreAccess, error) {
	return c.feedAccess(ctx, path, sfa.FeedRequest{AccessAction: "set", Visibility: v.String()}, opts)
}

// GrantFeedReader adds a peer to a feed's reader list.
func (c *Client) GrantFeedReader(ctx context.Context, path string, reader peer.ID, opts ...FeedOption) (*wire.StoreAccess, error) {
	return c.feedAccess(ctx, path, sfa.FeedRequest{AccessAction: "grant", ReaderPeerID: reader.String()}, opts)
}

// RevokeFeedReader removes a peer from a feed's reader list.
func (c *Client) RevokeFeedReader(ctx context.Context, path string, reader peer.ID, opts ...FeedOption) (*wire.StoreAccess, error) {
	return c.feedAccess(ctx, path, sfa.FeedRequest{AccessAction: "revoke", ReaderPeerID: reader.String()}, opts)
}

func (c *Client) feedAccess(ctx context.Context, path string, req sfa.FeedRequest, opts []FeedOption) (*wire.StoreAccess, error) {
	req.Operation = sfa.OpACCESS
	req.OwnerPeerID = c.host.ID().String()
	req.Path = path
	resp, err := c.doFeed(ctx, &req, opts)
	if err != nil {
		return nil, err
	}
	return decodeAccess("feed access", resp.Status, resp.Headers, resp.Body)
}

// ---------------------------------------------------------------------------
// Collections
// ---------------------------------------------------------------------------

// CollectionAccess reports a collection's visibility and reader list.
func (c *Client) CollectionAccess(ctx context.Context, path string, opts ...CollectionOption) (*wire.StoreAccess, error) {
	return c.collectionAccess(ctx, path, sca.CollectionRequest{AccessAction: "get"}, opts)
}

// SetCollectionVisibility makes a collection private, shared or public.
func (c *Client) SetCollectionVisibility(ctx context.Context, path string, v wire.Visibility, opts ...CollectionOption) (*wire.StoreAccess, error) {
	return c.collectionAccess(ctx, path, sca.CollectionRequest{AccessAction: "set", Visibility: v.String()}, opts)
}

// GrantCollectionReader adds a peer to a collection's reader list.
func (c *Client) GrantCollectionReader(ctx context.Context, path string, reader peer.ID, opts ...CollectionOption) (*wire.StoreAccess, error) {
	return c.collectionAccess(ctx, path, sca.CollectionRequest{AccessAction: "grant", ReaderPeerID: reader.String()}, opts)
}

// RevokeCollectionReader removes a peer from a collection's reader list.
func (c *Client) RevokeCollectionReader(ctx context.Context, path string, reader peer.ID, opts ...CollectionOption) (*wire.StoreAccess, error) {
	return c.collectionAccess(ctx, path, sca.CollectionRequest{AccessAction: "revoke", ReaderPeerID: reader.String()}, opts)
}

func (c *Client) collectionAccess(ctx context.Context, path string, req sca.CollectionRequest, opts []CollectionOption) (*wire.StoreAccess, error) {
	req.Operation = sca.OpACCESS
	req.OwnerPeerID = c.host.ID().String()
	req.Path = path
	resp, err := c.doCollection(ctx, &req, opts)
	if err != nil {
		return nil, err
	}
	return decodeAccess("collection access", resp.Status, resp.Headers, resp.Body)
}

// decodeAccess turns an ACCESS reply into the visibility and reader list,
// or the server's refusal.
func decodeAccess(op string, status int, headers map[string]any, body string) (*wire.StoreAccess, error) {
	if status >= 400 {
		return nil, responseError(op, status, headers)
	}
	raw, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return nil, fmt.Errorf("decode %s body: %w", op, err)
	}
	var access wire.StoreAccess
	if err := json.Unmarshal(raw, &access); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", op, err)
	}
	return &access, nil
}
