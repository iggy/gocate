package index

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Progress is a snapshot of an indexing run in flight, delivered to the
// Options.Progress callback.
type Progress struct {
	// Root is the tree being indexed.
	Root string
	// Scanned counts the entries the walk has visited, including directories
	// and entries that were skipped or turned out to be unchanged. While
	// Counting is set it holds the pre-walk's count instead.
	Scanned int
	// Total is how many entries the tree held when it was counted up front
	// (Options.CountTotal). It is 0 when no total is known, which is the right
	// thing to render as "no total": files can appear or disappear during a run,
	// so the counted number is a snapshot, not a promise.
	Total int
	// Counting is set while the optional counting pre-walk is still running and
	// no hashes, rows or skips have been produced yet.
	Counting bool
	// Skipped counts the files quick mode left untouched because the store
	// already holds a row for them.
	Skipped int
	// Hashed counts the regular files whose content has been hashed.
	Hashed int
	// Indexed counts the rows accepted into a write batch, inserted or updated.
	// Rows found unchanged are dropped instead, so they are not counted here.
	Indexed int
	// Bytes is the total size of the files hashed so far.
	Bytes int64
	// Elapsed is how long the run has been going.
	Elapsed time.Duration
}

// tracker accumulates the counters reported through Options.Progress. It is
// always allocated, because incrementing it is cheaper than branching at every
// call site, and its methods are safe to call from the walk, the hasher pool
// and the consumer concurrently.
type tracker struct {
	start    time.Time
	total    atomic.Int64
	counted  atomic.Int64
	counting atomic.Bool
	scanned  atomic.Int64
	skipped  atomic.Int64
	hashed   atomic.Int64
	indexed  atomic.Int64
	bytes    atomic.Int64
}

func (t *tracker) progress(root string) Progress {
	p := Progress{
		Root:    root,
		Scanned: int(t.scanned.Load()),
		Total:   int(t.total.Load()),
		Skipped: int(t.skipped.Load()),
		Hashed:  int(t.hashed.Load()),
		Indexed: int(t.indexed.Load()),
		Bytes:   t.bytes.Load(),
		Elapsed: time.Since(t.start),
	}
	if t.counting.Load() {
		// The pre-walk is the only thing that has happened so far, so its
		// progress is the only interesting number to show.
		p.Scanned = int(t.counted.Load())
		p.Counting = true
	}
	return p
}

// countEntries walks the tree once without statting anything (WalkDir takes the
// entry type from the directory listing) to establish the total a progress
// display divides by. Callers opt in to it: the pass is negligible next to
// hashing, but it doubles the cost of a metadata-only (-no-hash) run over a huge
// or slow tree.
func countEntries(ctx context.Context, root string, trk *tracker) error {
	trk.counting.Store(true)
	defer trk.counting.Store(false)

	// Visit the same set of entries as filepath.Walk does, so the total matches
	// what the run goes on to scan.
	err := filepath.WalkDir(root, func(_ string, _ fs.DirEntry, err error) error {
		if err != nil {
			return nil // the real walk reports it; a short total is still useful
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		trk.counted.Add(1)
		return nil
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("count %q: %w", root, err)
	}
	trk.total.Store(trk.counted.Load())
	return nil
}

// newReporter starts sending a Progress snapshot to fn every interval until the
// returned stop function is called. stop delivers one final snapshot before it
// returns, so a caller that prints a summary right afterwards never reports
// fewer rows than were actually indexed. Every call to fn happens on a single
// goroutine and calls never overlap, so fn needs no locking of its own.
func newReporter(root string, t *tracker, interval time.Duration, fn func(Progress)) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fn(t.progress(root))
			case <-done:
				fn(t.progress(root))
				return
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}
}
