package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	client "github.com/twostack/go-ricochet/pkg/client"
)

// infoLinesSince returns the Info lines the server logged after the marker.
func infoLinesSince(logs, marker string) []string {
	var out []string
	for _, line := range strings.Split(logs[len(marker):], "\n") {
		if strings.Contains(line, "level=INFO") {
			out = append(out, line)
		}
	}
	return out
}

// A request on the hot path is one Info line: the access line at the top of
// the pipeline. It used to be two to five, one per stage that felt like
// narrating, which at a few thousand requests a second is the log's whole
// budget spent on saying nothing an operator can use.
func TestOneInfoLinePerRequest(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)
	self := cl.PeerID()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The first request creates the mailbox and warms the connection; the
	// lines it costs are not the steady state.
	if _, err := cl.SendMessage(ctx, self, []byte("warm-up")); err != nil {
		t.Fatalf("warm-up: %v", err)
	}

	steps := []struct {
		name, protocol, op string
		status             string
		do                 func() error
	}{
		{"submit", "msa", "submit", "200", func() error {
			res, err := cl.SendMessage(ctx, self, []byte("measured"))
			if err == nil && !res.Success {
				err = res.Err()
			}
			return err
		}},
		{"retrieve", "maa", "retrieve", "200", func() error {
			_, err := cl.RetrieveMessages(ctx, client.WithMaxMessages(10))
			return err
		}},
		{"document put", "sda", "PUT", "201", func() error {
			_, err := cl.PutDocument(ctx, self, "access/log", []byte("{}"))
			return err
		}},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			marker := server.Logs()
			if err := step.do(); err != nil {
				t.Fatalf("%s: %v", step.name, err)
			}
			lines := infoLinesSince(server.Logs(), marker)
			if len(lines) != 1 {
				t.Fatalf("%s logged %d Info lines, want 1:\n%s", step.name, len(lines), strings.Join(lines, "\n"))
			}
			line := lines[0]
			for _, want := range []string{"msg=request", "protocol=" + step.protocol, "op=" + step.op, "status=" + step.status, "peer=" + self.String(), "duration="} {
				if !strings.Contains(line, want) {
					t.Errorf("access line lacks %q: %s", want, line)
				}
			}
		})
	}
}
