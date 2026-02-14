package client

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/twostack/go-ricochet/internal/core"
)

func TestCompressDecompressRoundTrip(t *testing.T) {
	// Create a compressible payload above threshold.
	payload := []byte(strings.Repeat("hello world, this is a test message for compression! ", 50))

	compressed, flags, err := CompressPayload(payload, 0, DefaultCompressionThreshold)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if flags&core.FlagCompressed == 0 {
		t.Fatal("expected FlagCompressed to be set")
	}
	if len(compressed) >= len(payload) {
		t.Errorf("compression didn't reduce size: %d >= %d", len(compressed), len(payload))
	}

	decompressed, err := DecompressPayload(compressed, flags, MaxDecompressedSize)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decompressed, payload) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d", len(decompressed), len(payload))
	}
}

func TestCompressBelowThreshold(t *testing.T) {
	payload := []byte("short")

	result, flags, err := CompressPayload(payload, 0, DefaultCompressionThreshold)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if flags&core.FlagCompressed != 0 {
		t.Error("should not set FlagCompressed for below-threshold payload")
	}
	if !bytes.Equal(result, payload) {
		t.Error("below-threshold payload should be unchanged")
	}
}

func TestCompressIncompressibleData(t *testing.T) {
	// Random data is incompressible.
	payload := make([]byte, 2048)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	result, flags, err := CompressPayload(payload, 0, DefaultCompressionThreshold)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	// Should either pass through uncompressed or compress (both valid).
	if flags&core.FlagCompressed == 0 {
		if !bytes.Equal(result, payload) {
			t.Error("uncompressed result should equal original")
		}
	}
}

func TestDecompressNotCompressed(t *testing.T) {
	payload := []byte("not compressed")
	result, err := DecompressPayload(payload, 0, MaxDecompressedSize)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(result, payload) {
		t.Error("non-compressed payload should pass through unchanged")
	}
}

func TestDecompressTooShort(t *testing.T) {
	_, err := DecompressPayload([]byte{1, 2}, core.FlagCompressed, MaxDecompressedSize)
	if err == nil {
		t.Error("expected error for too-short compressed payload")
	}
}

func TestDecompressExceedsMaxSize(t *testing.T) {
	// Create a payload that claims to be very large.
	payload := make([]byte, 8)
	payload[0] = 0xFF
	payload[1] = 0xFF
	payload[2] = 0xFF
	payload[3] = 0xFF
	_, err := DecompressPayload(payload, core.FlagCompressed, 1024)
	if err == nil {
		t.Error("expected error for oversized decompressed payload")
	}
}
