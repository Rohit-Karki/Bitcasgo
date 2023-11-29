package bitcaspy

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"

	datafile "rohit.com/internal"
)

func (b *BitCaspy) get(key string) (Record, error) {
	meta, ok := b.KeyDir[key]
	if !ok {
		return Record{}, nil
	}

	var (
		Header Header
		reader *datafile.DataFile
	)
	reader = b.df
	if meta.id != b.df.ID() {
		reader, ok = b.stale[meta.id]
		if !ok {
			return Record{}, fmt.Errorf("error for looking for the key  in the file %s", meta.id)
		}
	}

	data, err := reader.Read(meta.value_pos, meta.value_sz)
	if err != nil {
		return Record{}, fmt.Errorf("Error reading the dat from database file %v", err)
	}

	//Decode the header
	if err := Header.Decode(data); err != nil {
		return Record{}, fmt.Errorf("Error decoding the header")
	}

	var (
		offset = meta.value_pos + meta.value_sz
		val    = data[offset:]
	)
	record := Record{
		Header: Header,
		Key:    key,
		Value:  val,
	}
	return record, nil
}

func (b *BitCaspy) put(df *datafile.DataFile, Key string, Value []byte, expiryTime *time.Time) error {
	// Prepare the header
	header := Header{
		crc:    crc32.ChecksumIEEE(Value),
		tstamp: uint32(time.Now().Unix()),
		ksz:    uint32(len(Key)),
		vsz:    uint32(len(Value)),
	}
	if expiryTime != nil {
		header.expiry = uint32(expiryTime.Unix())
	} else {
		header.expiry = 0
	}

	// Get the buffer from the pool for writing data.
	buf := b.bufPool.Get().(*bytes.Buffer)
	defer b.bufPool.Put(buf)

	defer buf.Reset()

	// Encode the header
	header.Encode(buf)

	// Set the keys and values
	buf.WriteString(Key)
	buf.Write(Value)

	offset, err := df.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("Error writing the Record to the data file: %v", err)
	}

	// Creating the meta objec of the keydir
	meta := Meta{
		id:        df.ID(),
		value_sz:  len(Value),
		value_pos: offset,
		tstamp:    int(header.tstamp),
	}

	b.KeyDir[Key] = meta

	// Ensure that the inmemory data of the buffer is always pushed onto the disk
	if err := df.Sync(); err != nil {
		return fmt.Errorf("Error syncing the buffer to the disk: %v", err)
	}
	return nil
}

func (b *BitCaspy) delete(Key string) error {
	if err := b.put(b.df, Key, nil, nil); err != nil {
		return fmt.Errorf("Error deleting the key: %v", err)
	}
	delete(b.KeyDir, Key)
	return nil
}
