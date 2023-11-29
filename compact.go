package bitcaspy

import (
	"fmt"
	"os"
	"time"

	datafile "rohit.com/internal"
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
	if size < 20000 {
		return nil
	}
	oldId := b.df.ID()

	b.stale[oldId] = b.df
	newDf, err := datafile.New("rohit", oldId+1)
	if err != nil {
		return err
	}
	b.df = newDf
	return nil
}

// Compacts the old stale data files
func (b *BitCaspy) compaction(evalInterval time.Duration) {
	var (
		evalTicker = time.NewTicker(evalInterval).C
	)
	for range evalTicker {
		b.Lock()

		if err := b.deleteIfExpired(); err != nil {
			fmt.Errorf("failed to delete the expired keys: %v", err)
		}

		// Merge the datafiles
		if err := b.merge(); err != nil {

		}

	}
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
	// Create a new datafile for storing the output of merged files.
	// Use a temp directory to store the file and move to main directory after merge is over.
	tmpMergeDir, err := os.MkdirTemp("", "merged")
	if err != nil {
		return err
	}

	newFile, err := datafile.New(tmpMergeDir, 0)
	if err != nil {
		fmt.Errorf("Error creating new datafile: %v", err)
	}

}
