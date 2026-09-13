package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/pkg/wire"
)

// When storage fails under a live server, the client learns that the
// request failed and nothing about the database behind it.
func TestStorageErrorsAreNotEchoedToClients(t *testing.T) {
	srv := newTestServer(t)
	c := newTestClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Every storage call from here on fails inside the driver.
	srv.Storage.Close()

	leaks := []string{"pool", "closed", "pgx", "SQLSTATE", "postgres", "mailboxes", "stored_messages"}
	check := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s succeeded with storage closed", what)
		}
		text := err.Error()
		if !strings.Contains(text, "internal error") {
			t.Errorf("%s error %q does not carry the fixed phrase", what, text)
		}
		for _, leak := range leaks {
			if strings.Contains(strings.ToLower(text), leak) {
				t.Errorf("%s error %q leaks %q", what, text, leak)
			}
		}
	}

	check("create mailbox", c.CreateMailbox(ctx, "leaky", wire.MailboxPrivate))

	result, err := c.SendMessage(ctx, srv.PeerID, []byte("hello"))
	if err == nil {
		err = result.Err()
	}
	check("send message", err)
}
