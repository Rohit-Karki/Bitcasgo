package bitcasgo

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zerodha/logf"
	datafile "bitcasgo/internal"
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
			return nil, fmt.Errorf("applying option failed: %w", err)
		}
	}
	var (
		index  = 0
		lo     = initLogger(opts.debug)
		flockF *os.File
		stale  = map[int]*datafile.DataFile{}
	)

	// ensure data dir exists
	if !exists(opts.dir) {
		if err := os.MkdirAll(opts.dir, 0o755); err != nil {
			return nil, fmt.Errorf("error creating data dir %q: %w", opts.dir, err)
		}
	}

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
			flock, err := getFLock(lockFilePath)
			if err != nil {
				return nil, err
			}
			flockF = flock
		} else {
			flock, err := getFLock(lockFilePath)
			if err != nil {
				return nil, err
			}
			flockF = flock
		}
	}

	// Create a new active datafile
	df, err := datafile.New(opts.dir, index)
	if err != nil {
		return nil, fmt.Errorf("error creating new datafile: %v", err)
	}

	// Create a empty keyDirectory
	KeyDir := make(KeyDir, 0)

	// Initialize key directory from hint file if it exists
	hintPath := filepath.Join(opts.dir, HINTS_FILE)
	if err := KeyDir.Decode(hintPath); err != nil {
		lo.Error("Failed to decode hint file", "path", hintPath, "error", err)
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

	// background workers
	if !BitCaspy.opts.readOnly {
		go BitCaspy.runCompaction(BitCaspy.opts.compactInterval)
		go BitCaspy.checkFileSize(BitCaspy.opts.checkFileSizeInterval)
	}
	if BitCaspy.opts.syncInterval != nil && !BitCaspy.opts.alwaysFSync {
		interval := *BitCaspy.opts.syncInterval
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for range t.C {
				_ = BitCaspy.Sync()
			}
		}()
	}

	return BitCaspy, nil
}

func (b *BitCaspy) Close() error {
	b.Lock()
	defer b.Unlock()

	// Generate Hint files from the keydir
	if err := b.genrateHintFiles(); err != nil {
		b.lo.Error("Error generating Hint files from keydir", "error", err)
	}

	// Close the active data file
	if err := b.df.Close(); err != nil {
		b.lo.Error("Error closing active data file", "error", err)
	}

	// Close all the stale data files
	for _, df := range b.stale {
		if err := df.Close(); err != nil {
			b.lo.Error("Error closing stale data file", "error", err)
		}
	}
	if b.flockF != nil {
		if err := destroyFLock(b.flockF); err != nil {
			b.lo.Error("Error releasing file lock", "error", err)
		}
	}
	return nil
}

// Gets the key from the keydir and then checks for the key in the keydir hashmap
// Then it goes to the value offset in the data file
func (b *BitCaspy) Get(key string) ([]byte, error) {
	b.RLock()
	defer b.RUnlock()
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
	if key == "" {
		return ErrEmptyKey
	}
	if uint64(len(key)) > uint64(^uint32(0)) {
		return ErrLargeKey
	}
	if uint64(len(value)) > uint64(^uint32(0)) {
		return ErrLargeValue
	}
	b.Lock()
	defer b.Unlock()
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
	b.RLock()
	keys := make([]string, 0, len(b.KeyDir))
	for key := range b.KeyDir {
		keys = append(keys, key)
	}
	b.RUnlock()
	return keys
}

func (b *BitCaspy) Fold(foldingFunc func(key string, value []byte, acc string) error) error {
	// take a snapshot of keys under read lock
	b.RLock()
	keys := make([]string, 0, len(b.KeyDir))
	for key := range b.KeyDir {
		keys = append(keys, key)
	}
	b.RUnlock()

	for _, key := range keys {
		val, err := b.Get(key)
		if err != nil {
			return err
		}
		if err := foldingFunc(key, val, ""); err != nil {
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
