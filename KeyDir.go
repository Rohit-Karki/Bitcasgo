package bitcasgo

import (
	"encoding/gob"
	"os"
)

type KeyDir map[string]Meta

// Meta is stored as value in keyDir and keys are the keys in the database
type Meta struct {
	fileId     int
	RecordSize int
	RecordPos  int
	tstamp     int
}

func (k *KeyDir) Encode(fPath string) error {
	file, err := os.Create(fPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	if err := encoder.Encode(k); err != nil {
		return err
	}

	return nil
}

func (k *KeyDir) Decode(fPath string) error {
	file, err := os.Open(fPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	// Create a new map to decode into
	newKeyDir := make(KeyDir)
	if err := decoder.Decode(&newKeyDir); err != nil {
		return err
	}

	// Replace the current keydir with the decoded one
	*k = newKeyDir
	return nil
}
