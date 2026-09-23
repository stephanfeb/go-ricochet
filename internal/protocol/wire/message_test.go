package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/stephanfeb/go-ricochet/internal/mda/mailboxes"
	"github.com/stephanfeb/go-ricochet/internal/mta"
)

// A database error, however it was wrapped, reaches the client as a fixed
// phrase; the constraint, table and SQL state stay on the server.
func TestClientMessageHidesDatabaseErrors(t *testing.T) {
	pgErr := &pgconn.PgError{
		Severity:       "ERROR",
		Code:           "23505",
		Message:        `duplicate key value violates unique constraint "uq_mailbox_owner_folder"`,
		Detail:         "Key (owner_peer_id, folder_path)=(12D3, inbox) already exists.",
		TableName:      "mailboxes",
		ConstraintName: "uq_mailbox_owner_folder",
	}
	for _, err := range []error{
		pgErr,
		fmt.Errorf("deliver message: %w", fmt.Errorf("create mailbox: %w", pgErr)),
		errors.New("closed pool"),
		fmt.Errorf("query collection: %w", errors.New("failed to connect to `host=db user=ricochet database=ricochet`")),
	} {
		got := ClientMessage(err)
		if got != "internal error" {
			t.Errorf("ClientMessage(%v) = %q, want the fixed phrase", err, got)
		}
		for _, leak := range []string{"SQLSTATE", "constraint", "mailboxes", "pool", "host=", "23505"} {
			if strings.Contains(got, leak) {
				t.Errorf("client message %q leaks %q", got, leak)
			}
		}
		if status, _ := Classify(err); status != StatusInternalError {
			t.Errorf("Classify(%v) = %d, want %d", err, status, StatusInternalError)
		}
	}
}

// An error written for the client keeps its wording, and only that: the
// context a caller wrapped around it is dropped.
func TestClientMessageKeepsClientFacingErrors(t *testing.T) {
	full := &mailboxes.MailboxFullError{}
	cases := []struct {
		err    error
		status int
	}{
		{fmt.Errorf("deliver message: %w", full), StatusInsufficientStorage},
		{&mta.RateLimitError{PeerID: "12D3"}, StatusTooManyRequests},
		{&mta.ValidationError{Message: "payload too large: 9 bytes"}, StatusBadRequest},
		{&mta.ForwardingRefusedError{Reason: "forwarder is not a trusted peer"}, StatusForbidden},
	}
	for _, c := range cases {
		status, _ := Classify(c.err)
		if status != c.status {
			t.Errorf("Classify(%v) = %d, want %d", c.err, status, c.status)
		}
		var inner interface{ Error() string }
		switch {
		case errors.As(c.err, &full):
			inner = full
		default:
			inner = c.err
		}
		if got := ClientMessage(c.err); got != inner.Error() {
			t.Errorf("ClientMessage(%v) = %q, want %q", c.err, got, inner.Error())
		}
	}

	var syntax *json.SyntaxError
	err := json.Unmarshal([]byte("{nope"), &struct{}{})
	if !errors.As(err, &syntax) {
		t.Fatalf("expected a syntax error, got %v", err)
	}
	if status, _ := Classify(err); status != StatusBadRequest {
		t.Errorf("a JSON syntax error classifies as %d, want %d", status, StatusBadRequest)
	}
	if got := ClientMessage(err); got != syntax.Error() {
		t.Errorf("ClientMessage(syntax) = %q, want the decoder's wording", got)
	}
}
