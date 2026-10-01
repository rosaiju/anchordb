// Package record implements the checksummed, length-prefixed frame that every
// persistent AnchorDB record (WAL and checkpoint) is wrapped in.
//
// Frame layout (little-endian), see docs/architecture.md §4.1:
//
//	0  4  N           payload length
//	4  4  lenCRC      crc32c(bytes[0:4])
//	8  4  payloadCRC  crc32c(type || payload)
//	12 1  type
//	13 3  reserved (zero)
//	16 N  payload
//
// The length has its own checksum so that a reader can trust N before reading
// the payload. That is what lets recovery tell "this record runs past the end
// of the file" (a torn write) apart from "this length field is garbage"
// (corruption).
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// HeaderSize is the fixed size of a frame header.
	HeaderSize = 16
	// MaxPayload bounds a single frame: a maximum transaction (64 MiB) plus
	// slack for its own headers.
	MaxPayload = 64<<20 + 1<<10
)

var (
	// ErrIncomplete: the buffer ends before the frame does.
	ErrIncomplete = errors.New("record: incomplete frame")
	// ErrBadHeader: the length checksum does not match or reserved bytes are set.
	ErrBadHeader = errors.New("record: bad frame header")
	// ErrTooLarge: the (checksum-valid) length exceeds MaxPayload.
	ErrTooLarge = errors.New("record: frame too large")
	// ErrBadChecksum: the payload checksum does not match.
	ErrBadChecksum = errors.New("record: payload checksum mismatch")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC-32C of b. Exposed so other formats in this module
// use the same polynomial.
func Checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Frame is a decoded record. Payload aliases the buffer passed to Decode.
type Frame struct {
	Type    byte
	Payload []byte
}

// Append encodes a frame and appends it to dst.
func Append(dst []byte, typ byte, payload []byte) []byte {
	var h [HeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[4:8], Checksum(h[0:4]))
	h[12] = typ
	crc := crc32.Update(0, castagnoli, h[12:13])
	crc = crc32.Update(crc, castagnoli, payload)
	binary.LittleEndian.PutUint32(h[8:12], crc)
	dst = append(dst, h[:]...)
	return append(dst, payload...)
}

// PayloadLen validates the header at the start of buf and returns the payload
// length it declares. Streaming readers use it to learn how many more bytes to
// read before calling Decode.
func PayloadLen(buf []byte) (int, error) {
	if len(buf) < HeaderSize {
		return 0, ErrIncomplete
	}
	if binary.LittleEndian.Uint32(buf[4:8]) != Checksum(buf[0:4]) ||
		buf[13] != 0 || buf[14] != 0 || buf[15] != 0 {
		return 0, ErrBadHeader
	}
	n := binary.LittleEndian.Uint32(buf[0:4])
	if n > MaxPayload {
		return 0, ErrTooLarge
	}
	return int(n), nil
}

// Decode decodes the frame at the start of buf. It returns the frame and the
// number of bytes it occupies. Checks happen in the order documented in the
// architecture spec, so callers can classify failures reliably. Decode does
// not allocate.
func Decode(buf []byte) (Frame, int, error) {
	n, err := PayloadLen(buf)
	if err != nil {
		return Frame{}, 0, err
	}
	total := HeaderSize + n
	if len(buf) < total {
		return Frame{}, 0, ErrIncomplete
	}
	crc := crc32.Update(0, castagnoli, buf[12:13])
	crc = crc32.Update(crc, castagnoli, buf[HeaderSize:total])
	if crc != binary.LittleEndian.Uint32(buf[8:12]) {
		return Frame{}, 0, ErrBadChecksum
	}
	return Frame{Type: buf[12], Payload: buf[HeaderSize:total]}, total, nil
}
