package datafile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	ACTIVE_DATAFILE = "bitcaspy%d.db"
)

type DataFile struct {
	sync.RWMutex

	writer *os.File
	reader *os.File
	id     int

	offset int
}

// New initializes a new DataFile for storing the data in the database
// Only one datafile ca be active at a time
func New(dir string, index int) (*DataFile, error) {
	// If the new file is not already present because of filled file of previous operation otherwise use the present active file

	path := filepath.Join(dir, fmt.Sprintf(ACTIVE_DATAFILE, index))
	writer, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("error opening file for writing db: %w", err)
	}

	reader, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file for reading db: %w", err)
	}

	stat, err := writer.Stat()
	if err != nil {
		return nil, fmt.Errorf("error fetching file stats: %v", err)
	}

	df := &DataFile{
		writer: writer,
		reader: reader,
		id:     index,
		offset: int(stat.Size()),
	}
	return df, nil
}

func (d *DataFile) ID() int {
	return d.id
}

func (d *DataFile) Size() (int64, error) {
	stat, err := d.writer.Stat()
	if err != nil {
		return -1, fmt.Errorf("error getting file size: %v", err)
	}

	return stat.Size(), nil
}

func (d *DataFile) Read(pos int, size int) ([]byte, error) {
	start := int64(pos - size)

	record := make([]byte, size)

	n, err := d.reader.ReadAt(record, start)
	if err != nil {
		return nil, err
	}

	if n != size {
		return nil, fmt.Errorf("error fetching record, invalid size")
	}

	return record, nil
}

func (d *DataFile) Write(data []byte) (int, error) {
	if _, err := d.writer.Write(data); err != nil {
		return 0, err
	}

	offset := d.offset

	offset += len(data)

	return offset, nil
}

func (d *DataFile) Close() error {
	if err := d.writer.Close(); err != nil {
		return err
	}

	if err := d.reader.Close(); err != nil {
		return err
	}

	return nil
}

func (d *DataFile) Sync () error {
	return d.writer.Sync()
}