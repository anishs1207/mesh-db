//go:build !windows

package storage

import (
	"fmt"
	"os"
	"syscall"
)

func lockExclusive(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock: %w", err)
	}
	return nil
}

func unlockExclusive(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("flock unlock: %w", err)
	}
	return nil
}
