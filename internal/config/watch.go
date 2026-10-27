package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"time"
)

// Watcher polls a config file and reports content changes.
//
// Polling a content hash is deliberate: it needs no platform-specific notification
// API and is immune to editors that replace files through rename, which breaks
// inode-based watchers.
type Watcher struct {
	path     string
	interval time.Duration
	last     []byte
}

// NewWatcher snapshots the current content of path; only later changes are reported.
// Create it right after the initial Load so no edit falls between the two.
func NewWatcher(path string, interval time.Duration) *Watcher {
	return &Watcher{path: path, interval: interval, last: fileSum(path)}
}

// Run blocks until ctx is done. On every content change it loads the file and calls
// onChange, or onError if the new content is invalid; the caller keeps its running
// config in that case and the file is tried again once its content changes.
func (w *Watcher) Run(ctx context.Context, onChange func(*Config), onError func(error)) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		sum := fileSum(w.path)
		if sum == nil || bytes.Equal(sum, w.last) {
			continue
		}
		w.last = sum
		cfg, err := Load(w.path)
		if err != nil {
			onError(err)
			continue
		}
		onChange(cfg)
	}
}

func fileSum(path string) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	sum := sha256.Sum256(data)
	return sum[:]
}
