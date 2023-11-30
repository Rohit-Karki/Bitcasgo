package bitcaspy

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Exists returns true if the given path exists on the filesystem.
func exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	} else {
		return true
	}
}

// returns the list of files in the database directory
func getDataFiles(outDir string) ([]string, error) {
	if !exists(outDir) {
		return nil, fmt.Errorf("Error finding the file %s", outDir)
	}

	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", outDir))
	if err != nil {
		return nil, fmt.Errorf("Error getting files from the directory %v", err)
	}
	return files, nil
}

// Returns the list of sorted ids of the file
func getIds(files []string) ([]int, error) {
	ids := make([]int, 0)

	for _, file := range files {
		id, err := strconv.ParseInt((strings.TrimPrefix(strings.TrimSuffix(filepath.Base(file), ".db"), "bitcaspy_")), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Error parsing the files path: %v", err)
		}
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	return ids, nil
}
