package capacity

import (
	"errors"

	forge "github.com/stephanfeb/go-p2p-forge"
)

// ErrStorageFull refuses a write while the database is at or over
// max_storage_bytes. It clears when data is deleted or the budget raised,
// not with time, so it is reported as a 507 rather than a 503.
var ErrStorageFull = errors.New("server storage is full")

// OverBudget reports whether the latest sample puts the database at or past
// its budget. Before the first sample it is false: refusing every write
// until a scan of the database completes would turn a slow startup into an
// outage. The sample is periodic, so the gate lags reality by one cleanup
// interval, which is what bounds how far past the budget a burst can go.
func (s *Sampler) OverBudget() bool {
	stats := s.Latest()
	if stats == nil || s.maxStorageBytes <= 0 {
		return false
	}
	return stats.DatabaseBytes >= s.maxStorageBytes
}

// WriteGate refuses write requests while the server is over budget.
//
// isWrite classifies the raw request frame, the same function the dual-rate
// limiter uses, so the gate sits after frame decoding and costs one string
// scan for reads. A nil isWrite treats every request as a write, which is
// right for the submission protocols where there is nothing else. A nil
// sampler admits everything: max_storage_bytes was the operator's number,
// and a server wired without a sampler has nothing to compare it to.
func WriteGate(s *Sampler, isWrite func(raw []byte) bool) forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		if s != nil && (isWrite == nil || isWrite(sc.RawBytes)) && s.OverBudget() {
			sc.Err = ErrStorageFull
			sc.Logger.Warn("write refused: storage over budget",
				"peer", sc.PeerID,
				"databaseBytes", s.Latest().DatabaseBytes,
				"maxStorageBytes", s.maxStorageBytes,
			)
			return
		}
		next()
	}
}
