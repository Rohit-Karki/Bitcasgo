package bitcaspy

import (
	"encoding/gob"
	"os"
)

type KeyDir map[string]Meta

type Meta struct {
	id        int
	value_sz  int
	value_pos int
	tstamp    int
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
	file, err := os.Create(fPath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	if err := decoder.Decode(k); err != nil {
		return err
	}

	return nil
}
