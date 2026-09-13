package wire

// Status codes carried in responses and reported by the client's error types.
// They are HTTP's, because the semantics line up and every client library and
// operator already knows them.
const (
	StatusOK                 = 200
	StatusBadRequest         = 400
	StatusForbidden          = 403
	StatusNotFound           = 404
	StatusConflict           = 409
	StatusTooManyRequests    = 429
	StatusInternalError      = 500
	StatusServiceUnavailable = 503

	// StatusInsufficientStorage is a full mailbox or a full server. It is
	// distinct from a 503: retrying will not help until the owner drains the
	// mailbox, so a client that treats it as backpressure would retry forever
	// against a condition only the recipient can clear.
	StatusInsufficientStorage = 507
)

// RetryAfterHeaderKey is the response header that carries a retry hint in
// milliseconds alongside a 429 or 503.
const RetryAfterHeaderKey = "Retry-After-Ms"
