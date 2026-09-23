package client

import (
	"encoding/binary"
	"fmt"

	"github.com/pierrec/lz4/v4"

	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

const (
	// DefaultCompressionThreshold is the minimum payload size to attempt compression.
	DefaultCompressionThreshold = 1024

	// MaxDecompressedSize is the maximum allowed decompressed payload size (16MB).
	MaxDecompressedSize = 16 * 1024 * 1024
)

// CompressPayload compresses the payload using LZ4 block compression if it exceeds
// the threshold. Returns the (possibly compressed) payload and updated flags.
func CompressPayload(payload []byte, flags wire.SFMessageFlags, threshold int) ([]byte, wire.SFMessageFlags, error) {
	if len(payload) < threshold {
		return payload, flags, nil
	}

	// Allocate buffer for compressed data: 4 bytes for original size + max compressed size.
	maxDst := lz4.CompressBlockBound(len(payload))
	buf := make([]byte, 4+maxDst)

	// Store original size as big-endian uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))

	// Compress into buf[4:].
	n, err := lz4.CompressBlock(payload, buf[4:], nil)
	if err != nil {
		return nil, 0, fmt.Errorf("lz4 compress: %w", err)
	}

	// If compression didn't help (n==0 means incompressible, or result >= original), skip.
	if n == 0 || n >= len(payload) {
		return payload, flags, nil
	}

	return buf[:4+n], flags | wire.FlagCompressed, nil
}

// DecompressPayload decompresses an LZ4-compressed payload if the compressed flag is set.
func DecompressPayload(payload []byte, flags wire.SFMessageFlags, maxSize int) ([]byte, error) {
	if flags&wire.FlagCompressed == 0 {
		return payload, nil
	}

	if len(payload) < 4 {
		return nil, fmt.Errorf("compressed payload too short: %d bytes", len(payload))
	}

	originalSize := binary.BigEndian.Uint32(payload[:4])
	if int(originalSize) > maxSize {
		return nil, fmt.Errorf("decompressed size %d exceeds max %d", originalSize, maxSize)
	}

	dst := make([]byte, originalSize)
	n, err := lz4.UncompressBlock(payload[4:], dst)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}

	return dst[:n], nil
}
