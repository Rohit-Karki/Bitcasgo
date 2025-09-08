package bitcasgo

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	datafile "bitcasgo/internal"
)

// checkFileSize delegates to rotateDf checks the file size for the mac file size
// then places it into stale data files and creates a new data file
func (b *BitCaspy) checkFileSize(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)

	for range evalTicker {
		if err := b.rotateDf(); err != nil {
			fmt.Errorf("failed to scan the active file: %v", err)
		}
	}
}

// RunCompaction runs cleanup process to compact the keys and cleanup
// dead/expired keys at a periodic interval. This helps to save disk space
// and merge old inactive db files in a single file. It also generates a hints file
// which helps in caching all the keys during a cold start.
func (b *BitCaspy) runCompaction(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)
	for range evalTicker {
		if err := b.deleteIfExpired(); err != nil {
			b.lo.Error("Error deleting expired datafiles", "error", err)
		}
		if err := b.merge(); err != nil {
			b.lo.Error("Error merging stale datafiles", "error", err)
		}
		if err := b.genrateHintFiles(); err != nil {
			b.lo.Error("Error generating hint file", "error", err)
		}
	}
}

// rotateDf checks the file size for the mac file size
// then places it into stale data files and creates a new data file
func (b *BitCaspy) rotateDf() error {
	b.Lock()
	defer b.Unlock()

	size, err := b.df.Size()
	if err != nil {
		return err
	}

	// If smaller than threshold no action
	if size < b.opts.maxActiveFileSize {
		return nil
	}
	oldId := b.df.ID()

	b.stale[oldId] = b.df
	newDf, err := datafile.New(b.opts.dir, oldId+1)
	if err != nil {
		return err
	}
	b.df = newDf
	return nil
}

// Encode the keyDir hashmap into gob
func (b *BitCaspy) genrateHintFiles() error {
	hintFile := filepath.Join(b.opts.dir, HINTS_FILE)

	err := b.KeyDir.Encode(hintFile)
	if err != nil {
		return err
	}
	return nil
}

func (b *BitCaspy) deleteIfExpired() error {
	// Iterate over all keys and delete all keys which are expired.
	keyDir := b.KeyDir
	for k := range keyDir {
		record, err := b.get(k)
		if err != nil {
			fmt.Errorf("error %v", err)
		}
		if record.isExpired() {
			if err := b.delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *BitCaspy) merge() error {
	// Only merge when stale datafiles are more than 2
	if len(b.stale) < 2 {
		return nil
	}
	// Create a new datafile for storing the output of merged files.
	// Use a temp directory to store the file and move to main directory after merge is over.
	tmpMergeDir, err := os.MkdirTemp("", "merged")
	if err != nil {
		return err
	}

	defer os.RemoveAll(tmpMergeDir)
	newFile, err := datafile.New(tmpMergeDir, 0)
	if err != nil {
		b.lo.Error("Error creating new datafile", "error", err)
	}

	// Loop over all the active keys from the keydir and
	// Since the keydir has updated values of all keys, all the old keys which are expired/deleted/overwritten
	// will be cleaned up in the merged database.

	for k := range b.KeyDir {
		record, err := b.get(k)
		if err != nil {
			return err
		}

		if err := b.put(newFile, k, record.Value, nil); err != nil {
			return err
		}
	}

	// close all the stale datafiles because all the stale datafiles are merged into new datafile
	for _, df := range b.stale {
		if err := df.Close(); err != nil {
			return err
		}
	}

	// Reset the stale hashmap to none because all the stale datafiles are merged into new datafile
	b.stale = make(map[int]*datafile.DataFile, 0)

	// Delete all old .db datafiles
	err = filepath.Walk(b.opts.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) == ".db" {
			err := os.Remove(path)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Move the new merged datafile to the old database directory and delete all old datafiles
	os.Rename(filepath.Join(tmpMergeDir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)),
		filepath.Join(b.opts.dir, fmt.Sprintf(datafile.ACTIVE_DATAFILE, 0)))

	b.df = newFile

	b.df.Sync()
	return nil
}
