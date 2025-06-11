package bitcasgo

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"

	datafile "bitcasgo/internal"
)

func (b *BitCaspy) get(key string) (Record, error) {
	meta, ok := b.KeyDir[key]
	fmt.Println("Meta: ", meta)
	if !ok {
		return Record{}, ErrNoKey
	}

	var (
		header Header
		reader *datafile.DataFile
	)
	reader = b.df
	// Isnot in Active data file then go to stale data files
	if meta.fileId != b.df.ID() {
		reader, ok = b.stale[meta.fileId]
		if !ok {
			return Record{}, fmt.Errorf("error for looking for the key  in the file %s", meta.fileId)
		}
	}

	// Read header first
	data, err := reader.Read(meta.RecordPos, meta.RecordSize)
	// headerData, err := reader.Read(meta.value_pos, int(binary.Size(header)))
	if err != nil {
		return Record{}, fmt.Errorf("Error reading header from database file: %v", err)
	}

	// Decode the header
	if err := header.Decode(data); err != nil {
		return Record{}, fmt.Errorf("Error decoding header: %v", err)
	}
	fmt.Printf("Decoded header: %+v\n", header)
	var (
		valPos    = meta.RecordSize - int(header.Vsz)
		valueData = data[valPos:]
	)
	fmt.Println("Value Data: ", valueData)
	if err != nil {
		return Record{}, fmt.Errorf("Error reading value from database file: %v", err)
	}

	record := Record{
		Header: header,
		Key:    key,
		Value:  valueData,
	}
	return record, nil
}

func (b *BitCaspy) put(df *datafile.DataFile, Key string, Value []byte, expiryTime *time.Time) error {

	// Prepare the header
	header := Header{
		Crc:    crc32.ChecksumIEEE(Value),
		Tstamp: uint32(time.Now().Unix()),
		Ksz:    uint32(len(Key)),
		Vsz:    uint32(len(Value)),
	}
	fmt.Println("Header: ", header.Crc, header.Tstamp, header.Ksz, header.Vsz)
	if expiryTime != nil {
		header.Expiry = uint32(expiryTime.Unix())
	} else {
		header.Expiry = 0
	}

	record := Record{
		Key:   Key,
		Value: Value,
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
	fmt.Println("Offset: ", offset)
	if err != nil {
		return fmt.Errorf("Error writing the Record to the data file: %v", err)
	}

	// Creating the meta object of the keydir
	meta := Meta{
		fileId:     df.ID(),
		RecordSize: len(buf.Bytes()),
		RecordPos:  offset + len(buf.Bytes()),
		tstamp:     int(record.Header.Tstamp),
	}
	fmt.Println("Meta: ", meta)

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
