package client

import (
	"io"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/stephanfeb/go-ricochet/internal/protocol/frame"
	"github.com/stephanfeb/go-ricochet/internal/protocol/notify"
	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

// NotificationHandler is called when a push notification is received. The
// notification type is wire.Notification, so handlers can be written outside
// this module.
type NotificationHandler func(*wire.Notification)

// RegisterNotificationHandler registers a handler for incoming push notifications.
// The handler is called on a separate goroutine for each notification.
func (c *Client) RegisterNotificationHandler(handler NotificationHandler) {
	c.notifying.Store(true)
	c.host.SetStreamHandler(notify.ProtocolID, func(s network.Stream) {
		defer s.Close()

		data, err := frame.ReadFrame(s)
		if err != nil {
			if err != io.EOF {
				slog.Debug("failed to read notification", "error", err)
			}
			return
		}

		n, err := wire.DecodeNotification(data)
		if err != nil {
			slog.Debug("failed to decode notification", "error", err)
			return
		}

		go handler(n)
	})
}
