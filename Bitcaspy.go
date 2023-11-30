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
	// 	go BitCaspy
	// }

	return BitCaspy, nil
}
