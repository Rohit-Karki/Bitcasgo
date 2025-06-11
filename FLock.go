package bitcasgo

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func getFLock(flockfile string) (*os.File, error) {
	flockF, err := os.Create(flockfile)
	if err != nil {
		return nil, fmt.Errorf("cannot create lock file %q: %w", flockF, err)
	}

	if err := unix.Flock(int(flockF.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("cannot acquire lock on file %q: %w", flockF, err)
	}

	return flockF, nil
}

func destroyFLock(flockF *os.File) error {
	if err := unix.Flock(int(flockF.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("cannot unlock lock on file %q: %w", flockF.Name(), err)
	}
	// Close any open fd.
	if err := flockF.Close(); err != nil {
		return fmt.Errorf("cannot close fd on file %q: %w", flockF.Name(), err)
	}
	// Remove the lock file from the filesystem.
	if err := os.Remove(flockF.Name()); err != nil {
		return fmt.Errorf("cannot remove file %q: %w", flockF.Name(), err)
	}
	return nil
}
