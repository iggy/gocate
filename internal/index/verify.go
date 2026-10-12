package index

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/iggy/gocate/internal/store"
)

// VerifyStatus classifies how a file on disk agrees or disagrees with its
// database row.
type VerifyStatus int

const (
	// StatusMatching means the current content is confirmed to be what the row
	// records. It is the zero value because it is the only status that is not
	// reported.
	StatusMatching VerifyStatus = iota
	// StatusModified marks a file whose content differs from the recorded
	// hashes, including files the index holds no hashes for (rows written by
	// -no-hash) and files that cannot be read, since neither can be confirmed.
	StatusModified
	// StatusUnindexed marks a regular file under the verified root for which
	// this host has no row at all.
	StatusUnindexed
)

// String renders the status as the word used in command output.
func (v VerifyStatus) String() string {
	switch v {
	case StatusMatching:
		return "matching"
	case StatusModified:
		return "modified"
	case StatusUnindexed:
		return "unindexed"
	}
	return "unknown"
}

// VerifyDrift is one file that does not agree with the index.
type VerifyDrift struct {
	Path   string
	Status VerifyStatus
}

// Verify reports how the tree rooted at root disagrees with the rows stored for
// this host. It is the convenience form of VerifyCtx that cannot be cancelled.
func Verify(s *store.Store, root string) ([]VerifyDrift, error) {
	return VerifyCtx(context.Background(), s, root)
}

// VerifyCtx compares every regular file under root to the hashes stored for it
// and returns the files that disagree. It never writes to the database, so an
// empty report means the on-disk tree still matches the index and a non-empty
// one is exactly what -updatedb would record differently.
//
// Each file is hashed as little as the answer allows: files with no row are
// reported without being read at all, and an imohash mismatch is already proof
// of different content, so the full-content hash is only computed for files
// whose samples still match. That keeps the cost of an unchanged tree to one
// sequential read per file and the cost of a mostly-new tree to a walk.
//
// Rows whose file no longer exists are not reported: only disk-side drift
// (modified content and unindexed files) is in scope. Files are hashed by a
// bounded set of workers, and the report is sorted by path so repeated runs on
// an unchanged tree produce identical output.
//
// A cancelled walk stops early but still returns the drift found so far along
// with ctx.Err().
func VerifyCtx(ctx context.Context, s *store.Store, root string) ([]VerifyDrift, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve path %q: %w", root, err)
	}

	// Single full-table read: the snapshot is read-only from here on and is
	// shared between the walk and the hashing workers without locking, which is
	// what lets the walk decide "unindexed" without a per-row SELECT.
	existing, err := s.LoadExisting()
	if err != nil {
		return nil, fmt.Errorf("load existing rows: %w", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var drift []VerifyDrift

	// Bounded hasher pool + byte budget (same machinery as RunCtx).
	workers := runtime.NumCPU()
	budget := newByteBudget(defaultMaxBytes(workers))

	semCount := make(chan struct{}, workers)

	add := func(path string, status VerifyStatus) {
		mu.Lock()
		drift = append(drift, VerifyDrift{Path: path, Status: status})
		mu.Unlock()
	}

	walkErr := filepath.Walk(abs, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("walk error")
			return nil // skip this entry, keep walking
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !info.Mode().IsRegular() {
			return nil // symlinks and directories have no row to disagree with
		}

		old, recorded := existing[path]
		if !recorded {
			// A missing row is drift whatever the file contains, so there is
			// nothing worth reading.
			add(path, StatusUnindexed)
			return nil
		}

		wg.Add(1)
		select {
		case semCount <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			return ctx.Err()
		}
		charge := residentBytes(info.Size())
		if err := budget.acquire(ctx, charge); err != nil {
			<-semCount
			wg.Done()
			return err
		}
		go func() {
			defer wg.Done()
			defer func() { <-semCount }()
			defer budget.release(charge)

			status, vErr := compareContents(path, old)
			if vErr != nil {
				log.Error().Err(vErr).Str("path", path).Msg("failed to hash file; treating as modified")
			}
			if status != StatusMatching {
				add(path, status)
			}
		}()
		return nil
	})

	wg.Wait()

	// Wake any budget waiters so they can observe the cancellation. (The
	// walk is over, so there are none in practice, but this keeps the condvar
	// honest against future callers of VerifyCtx on the same machinery.)
	budget.wakeAll()

	// Sort drift for deterministic output.
	sort.Slice(drift, func(i, j int) bool {
		return drift[i].Path < drift[j].Path
	})
	if walkErr != nil && !errors.Is(walkErr, context.Canceled) {
		return drift, fmt.Errorf("walk %q: %w", abs, walkErr)
	}
	return drift, walkErr
}

// compareContents reports how the current content of path agrees with the
// hashes recorded in stored.
//
// The cheap check runs first and is conclusive in one direction: imohash folds
// in the file size and its sample bytes, so a mismatch means the contents
// differ and no further reading can overturn that. A match proves nothing on
// its own (imohash samples), so confirming equality still takes a full pass.
func compareContents(path string, stored store.FileInfo) (status VerifyStatus, err error) {
	h, err := openContentHasher(path)
	if err != nil {
		return StatusModified, err
	}
	defer func() {
		if cerr := h.Close(); cerr != nil && err == nil {
			status, err = StatusModified, cerr
		}
	}()

	imo, err := h.sample()
	if err != nil {
		return StatusModified, err
	}
	if imo != stored.Imohash {
		return StatusModified, nil
	}
	if imo == "" {
		// Both the file and the row are empty: there is no content left to
		// disagree about.
		return StatusMatching, nil
	}

	xxh, err := h.full()
	if err != nil {
		return StatusModified, err
	}
	if xxh != stored.XXH3Hash {
		return StatusModified, nil
	}
	return StatusMatching, nil
}
