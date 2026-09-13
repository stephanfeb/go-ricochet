package wire

import (
	"errors"
	"strings"
	"testing"
)

// A field over its bound or not UTF-8 is refused as a 400 that names the
// field; a field at the bound, or empty, passes.
func TestCheckString(t *testing.T) {
	if err := CheckString("title", strings.Repeat("a", 10), 10); err != nil {
		t.Fatalf("value at the bound refused: %v", err)
	}
	if err := CheckString("title", "", 10); err != nil {
		t.Fatalf("empty value refused: %v", err)
	}

	err := CheckString("title", strings.Repeat("a", 11), 10)
	var fe *FieldError
	if !errors.As(err, &fe) || fe.Field != "title" {
		t.Fatalf("over-long value gave %v, want a FieldError for title", err)
	}
	if status, _ := Classify(err); status != StatusBadRequest {
		t.Fatalf("FieldError classified as %d, want %d", status, StatusBadRequest)
	}
	if got := ClientMessage(err); !strings.Contains(got, "title") || !strings.Contains(got, "10") {
		t.Fatalf("client message %q does not name the field and bound", got)
	}

	if err := CheckString("bio", "ok\xff", 10); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
	// Bytes, not runes: a multibyte string is measured as stored.
	if err := CheckString("bio", strings.Repeat("é", 6), 10); err == nil {
		t.Fatal("12 bytes of UTF-8 accepted under a bound of 10")
	}
}

func TestCheckJSONSize(t *testing.T) {
	if err := CheckJSONSize("extras", nil, 10); err != nil {
		t.Fatalf("nil refused: %v", err)
	}
	if err := CheckJSONSize("extras", map[string]any{"a": 1}, 10); err != nil {
		t.Fatalf("small value refused: %v", err)
	}
	if err := CheckJSONSize("extras", map[string]any{"a": strings.Repeat("x", 20)}, 10); err == nil {
		t.Fatal("large value accepted")
	}
	if err := CheckJSONSize("extras", map[string]any{"f": func() {}}, 10); err == nil {
		t.Fatal("unencodable value accepted")
	}
}
