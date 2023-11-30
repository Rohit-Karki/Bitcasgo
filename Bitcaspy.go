package bitcaspy

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/zerodha/logf"
	datafile "rohit.com/internal"
)

const (
	LOCKFILE   = "bitcaspy.lock"
	HINTS_FILE = "bitcaspy.hints"
)

type BitCaspy struct {
	sync.RWMutex

	lo      logf.Logger
	bufPool sync.Pool
	opts    *Options

	KeyDir KeyDir                     // Hashmap of keys and location of the value for lookup
	df     *datafile.DataFile         // Active Data file where put operation is performed
	stale  map[int]*datafile.DataFile // stale is the hashmap of fileId and datafile which arenot currently active for put operation
	flockF *os.File                   // Lock for performing file lock
}

func initLogger(debug bool) logf.Logger {
	opts := logf.Opts{EnableCaller: true}
	if debug {
		opts.Level = logf.DebugLevel
	}
	return logf.New(opts)
}

func Init(cfg ...Config) (*BitCaspy, error) {
	// Set options
	opts := DefaultOptions()

	for _, opt := range cfg {
		if err := opt(opts); err != nil {

		}
	}
	var (
		index  = 0
		lo     = initLogger(opts.debug)
		flockF *os.File
		stale  = map[int]*datafile.DataFile{}
	)

	// load existing data files
	datafiles, err := getDataFiles(opts.dir)

	if err != nil {
		return nil, fmt.Errorf("error parsing ids for existing files: %w", err)
	}
	if len(datafiles) > 0 {
		ids, err := getIds(datafiles)

		if err != nil {
			return nil, fmt.Errorf("error getting existing ids for %s: %w", datafiles, err)
		}

		index = len(ids)

		// Create a in memory datafile from the existing disk datafile
		for _, id := range ids {
			df, err := datafile.New(opts.dir, id)
			if err != nil {
				return nil, fmt.Errorf("Error creating datafile: %v", err)
			}
			stale[id] = df
		}
	}

	// If not in readonly mode, generate a lock file to ensure that only one process is allowed to access the active datafile
	if !opts.readOnly {
		lockFilePath := filepath.Join(opts.dir, LOCKFILE)
		if !exists(lockFilePath) {
			_, err := getFLock(lockFilePath)
			if err != nil {
				return nil, err
			}
		}
	}

	// Create a new active datafile
	df, err := datafile.New(opts.dir, index)
	if err != nil {
		fmt.Errorf("error creating new datafile: %v", err)
	}

	// Create a empty keyDirectory
	KeyDir := make(KeyDir, 0)

	//Check if there is a hint file which we will decode and put those hashmap from hint files to keydir
	hintPath := filepath.Join(opts.dir, HINTS_FILE)
	if exists(hintPath) {
		if err := KeyDir.Decode(hintPath); err != nil {
			return nil, fmt.Errorf("failed to decode hint file %s: %v", hintPath, err)
		}
	}

	BitCaspy := &BitCaspy{
		lo: lo,
		bufPool: sync.Pool{New: func() any {
			return bytes.NewBuffer([]byte{})
		}},
		opts: opts,

		KeyDir: KeyDir,
		df:     df,
		stale:  stale,
		flockF: flockF,
	}

	go BitCaspy.runCompaction(BitCaspy.opts.compactInterval)

	go BitCaspy.checkFileSize(BitCaspy.opts.checkFileSizeInterval)

	// if BitCaspy.opts.syncInterval != nil{
	// 	go BitCaspy.syn
	// }

	return BitCaspy, nil
}

func (b *BitCaspy) Close() error {
	b.Lock()
	defer b.Unlock()

	// Generate Hint files from the keydir
	if err := b.genrateHintFiles(); err != nil {
		fmt.Errorf("Error generating Hint files from keydir: %v", err)
	}

	// Close the active data file
	if err := b.df.Close(); err != nil {
		fmt.Errorf("Error closing active data file: %v", err)
	}

	// Close all the stale data files
	for _, df := range b.stale {
		if err := df.Close(); err != nil {
			fmt.Errorf("Error closing stale data file: %v", err)
		}
	}
	return nil
}

// Gets the key from the keydir and then checks for the key in the keydir hashmap
// Then it goes to the value offset in the data file
func (b *BitCaspy) Get(key string) ([]byte, error) {
	record, err := b.get(key)
	if err != nil {
		return nil, err
	}
	if record.isExpired() {
		return nil, ErrExpiredKey
	}
	if !record.isValidChecksum() {
		return nil, ErrChecksumMismatch
	}
	return record.Value, nil
}

// puts the key into the active data file and puts the key and inserts in the keyDir hashmap the fileId, vsize and offset at the data file
func (b *BitCaspy) Put(key string, value []byte) error {
	if b.opts.readOnly {
		return ErrReadOnly
	}
	//
	return b.put(b.df, key, value, nil)
}

func (b *BitCaspy) Delete(key string) error {
	b.Lock()
	defer b.Unlock()
	if b.opts.readOnly {
		return ErrReadOnly
	}
	return b.delete(key)
}

func (b *BitCaspy) list_keys() []string {
	b.Lock()
	defer b.Unlock()
	key_lists := make([]string, 0, len(b.KeyDir))

	for key := range b.KeyDir {
		key_lists = append(key_lists, key)
	}
	return key_lists
}

func (b *BitCaspy) Fold(foldingFunc func(key string, value []byte, acc string) error) error {
	b.Lock()
	defer b.Unlock()

	for key, _ := range b.KeyDir {
		value, err := b.Get(key)
		if err != nil {
			return err
		}
		if err := foldingFunc(key, value, "rohit"); err != nil {
			return err
		}
	}
	return nil
}

func (b *BitCaspy) Sync() error {
	b.Lock()
	defer b.Unlock()

	return b.df.Sync()
}
