package client

import (
	"io"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/twostack/go-ricochet/internal/protocol/frame"
	"github.com/twostack/go-ricochet/internal/protocol/notify"
)

// NotificationHandler is called when a push notification is received.
type NotificationHandler func(*notify.Notification)

// RegisterNotificationHandler registers a handler for incoming push notifications.
// The handler is called on a separate goroutine for each notification.
func (c *Client) RegisterNotificationHandler(handler NotificationHandler) {
	c.host.SetStreamHandler(notify.ProtocolID, func(s network.Stream) {
		defer s.Close()

		data, err := frame.ReadFrame(s)
		if err != nil {
			if err != io.EOF {
				slog.Debug("failed to read notification", "error", err)
			}
			return
		}

		n, err := notify.DecodeNotification(data)
		if err != nil {
			slog.Debug("failed to decode notification", "error", err)
			return
		}

		go handler(n)
	})
}
