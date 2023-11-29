package bitcaspy

import (
	"os"
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

	KeyDir KeyDir
	df     *datafile.DataFile
	stale  map[int]*datafile.DataFile
	flockF *os.File
}

func initLogger(debug bool) logf.Logger {
	opts := logf.Opts{EnableCaller: true}
	if debug {
		opts.Level = logf.DebugLevel
	}
	return logf.New(opts)
}

func Init() (*BitCaspy, error) {
	var (
		index = 0
		flockF *os.File
		stale  = map[int]*datafile.DataFile{}
	)

	// load existing data files
	// file,err := 
}