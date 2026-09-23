package wire

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	public "github.com/stephanfeb/go-ricochet/pkg/wire"
)

// Field bounds, aliased from pkg/wire where clients can read them.
const (
	MaxContentTypeLength     = public.MaxContentTypeLength
	MaxCollectionNameLength  = public.MaxCollectionNameLength
	MaxCollectionKeyLength   = public.MaxCollectionKeyLength
	MaxFeedTitleLength       = public.MaxFeedTitleLength
	MaxFeedDescriptionLength = public.MaxFeedDescriptionLength
	MaxEntryTypeLength       = public.MaxEntryTypeLength
	MaxDisplayNameLength     = public.MaxDisplayNameLength
	MaxBioLength             = public.MaxBioLength
	MaxAvatarHashLength      = public.MaxAvatarHashLength
	MaxDirectoryExtrasBytes  = public.MaxDirectoryExtrasBytes
)

// FieldError is a request field the server will not store: too long, or
// not valid UTF-8. It is the client's to fix, so it classifies as a 400
// and its wording reaches the client.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string {
	return e.Field + " " + e.Reason
}

// CheckString refuses a string field over max bytes or not valid UTF-8.
// An empty value passes: whether a field is required is the handler's
// rule, this is only whether what was sent can be stored.
func CheckString(field, value string, max int) error {
	if len(value) > max {
		return &FieldError{Field: field, Reason: fmt.Sprintf("exceeds %d bytes", max)}
	}
	if !utf8.ValidString(value) {
		return &FieldError{Field: field, Reason: "is not valid UTF-8"}
	}
	return nil
}

// CheckJSONSize refuses a value whose JSON encoding is over max bytes. A
// nil value passes.
func CheckJSONSize(field string, value any, max int) error {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return &FieldError{Field: field, Reason: "cannot be encoded as JSON"}
	}
	if len(data) > max {
		return &FieldError{Field: field, Reason: fmt.Sprintf("exceeds %d bytes as JSON", max)}
	}
	return nil
}
