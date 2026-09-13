package wire

import (
	"testing"
	"time"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/forgetest"

	"github.com/twostack/go-ricochet/internal/core"
)

// message_timeout used to be parsed and read by nothing. It is now the
// deadline handlers see on sc.Ctx once the request has been read.
func TestRequestDeadline_BoundsTheHandlerContext(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.MessageTimeout = 250 * time.Millisecond
	reg := forge.NewRegistry()
	reg.Provide("config", cfg)

	var deadline time.Time
	var ok bool
	pipeline := forge.NewPipeline(nil,
		RequestDeadline(reg),
		func(sc *forge.StreamContext, next func()) {
			deadline, ok = sc.Ctx.Deadline()
		},
	).WithRegistry(reg)

	before := time.Now()
	pipeline.HandleStream(forgetest.NewMockStream(forgetest.GenerateTestPeerID(), nil))

	if !ok {
		t.Fatal("sc.Ctx carried no deadline")
	}
	if remaining := deadline.Sub(before); remaining > cfg.MessageTimeout+50*time.Millisecond || remaining < cfg.MessageTimeout-50*time.Millisecond {
		t.Fatalf("deadline %v from start; want about %v", remaining, cfg.MessageTimeout)
	}
}

// Without a config in the registry, or with a zero timeout, the forge
// request timeout is the only bound.
func TestRequestDeadline_ZeroLeavesTheContextAlone(t *testing.T) {
	cfg := core.DefaultConfig()
	cfg.MessageTimeout = 0
	reg := forge.NewRegistry()
	reg.Provide("config", cfg)

	var deadline time.Time
	pipeline := forge.NewPipeline(nil,
		RequestDeadline(reg),
		func(sc *forge.StreamContext, next func()) { deadline, _ = sc.Ctx.Deadline() },
	).WithRegistry(reg).WithRequestTimeout(time.Hour)
	pipeline.HandleStream(forgetest.NewMockStream(forgetest.GenerateTestPeerID(), nil))
	if time.Until(deadline) < 59*time.Minute {
		t.Fatalf("deadline %v; want the forge request timeout untouched", time.Until(deadline))
	}
}
