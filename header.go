package bitcasgo

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

type Record struct {
	Header Header
	Key    string
	Value  []byte
}

type Header struct {
	Crc    uint32
	Tstamp uint32
	Expiry uint32
	Ksz    uint32
	Vsz    uint32
}

func (h *Header) Encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

// Decode takes a record object decodes the binary value the buffer.
func (h *Header) Decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}

func (r *Record) isExpired() bool {
	if r.Header.Expiry == 0 {
		return false
	}
	return int64(r.Header.Expiry) < time.Now().Unix()
}

func (r *Record) isValidChecksum() bool {
	return crc32.ChecksumIEEE(r.Value) == r.Header.Crc
}
