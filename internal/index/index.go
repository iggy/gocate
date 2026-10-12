// Package index walks a filesystem tree and records file metadata and content
// hashes into a store.Store.
//
// The pipeline is a bounded producer/consumer fan-out: filepath.Walk produces
// dirents, a worker pool hashes regular files concurrently (capped by both
// goroutine count and total bytes in flight so a few huge files cannot crowd
// out small-file parallelism), and a single consumer goroutine writes results
// to the store in batched transactions.
//
// To avoid an O(n^2) sequence of per-row existence lookups against the
// database, Run loads the existing rows for the host into an in-memory map once
// and consults it instead. The map is read-only after construction and is
// shared between the walk and the consumer without locking.
//
// Callers that want to show what a long run is doing pass Options.Progress,
// which receives periodic snapshots of the pipeline's counters (progress.go).
package index

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/iggy/gocate/internal/store"
)

// Options controls how a tree is indexed.
type Options struct {
	// Hash, when true, computes content hashes for regular files. When false
	// (the -no-hash flag) only path/size/modtime are recorded.
	Hash bool
	// Quick, when true, skips files already present in the store (incremental
	// re-index) rather than re-hashing them.
	Quick bool
	// Workers is the maximum number of concurrent hashing goroutines. Values
	// <= 0 default to runtime.NumCPU().
	Workers int
	// MaxBytes bounds the file content the hashing workers hold at once, on top
	// of the Workers count cap. Charges are what a hash actually keeps resident
	// (the whole file below the imohash sample threshold, a fixed read buffer
	// above it), so a directory full of huge files still fills every worker.
	// Lower it to throttle reading on a slow mount. Values <= 0 default to one
	// worker's worth of content per worker.
	MaxBytes int64
	// BatchRows is how many rows the consumer accumulates before committing a
	// write transaction. <= 0 defaults to 1000.
	BatchRows int
	// BatchBytes bounds a write batch by the total content size of the files it
	// holds (sum of fi.Size), so a Ctrl-C that drops an in-flight batch never
	// forfeits more than this much hashing effort. <= 0 defaults to 4 GiB.
	BatchBytes int64
	// Progress, when non-nil, receives a snapshot of the run's counters every
	// ProgressInterval and once more when the run is over. It is called on a
	// dedicated goroutine and never concurrently with itself, so an
	// implementation only has to be safe against its own state. nil (the
	// default) means no reporting.
	Progress func(Progress)
	// ProgressInterval is how often Progress is called. <= 0 defaults to 500ms.
	// It is ignored when Progress is nil.
	ProgressInterval time.Duration
	// CountTotal makes the run walk the tree once, without statting or reading
	// anything, so Progress reports how many entries there are to visit. It only
	// pays for itself when someone is watching: <= an extra pass of readdir over
	// the tree, which is negligible next to hashing but doubles a metadata-only
	// (-no-hash) run.
	CountTotal bool
}

// Run indexes the tree rooted at root into s according to opts. It is the
// convenience form of RunCtx that cannot be cancelled.
func Run(s *store.Store, root string, opts Options) error {
	return RunCtx(context.Background(), s, root, opts)
}

// RunCtx indexes the tree rooted at root into s according to opts, stopping
// early when ctx is cancelled. A cancelled walk still flushes the in-flight
// write batch so partial progress is committed rather than lost, and the
// cancellation is returned as ctx.Err() so callers can tell an interrupted run
// from one that indexed the whole tree.
func RunCtx(ctx context.Context, s *store.Store, root string, opts Options) error {
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		// Room for every worker to hold the most content one hash can occupy.
		maxBytes = defaultMaxBytes(workers)
	}
	batchRows := opts.BatchRows
	if batchRows <= 0 {
		batchRows = 1000
	}
	batchBytes := opts.BatchBytes
	if batchBytes <= 0 {
		batchBytes = 4 << 30 // 4 GiB
	}

	trk := &tracker{start: time.Now()}
	stopReporting := func() {}
	if opts.Progress != nil {
		interval := opts.ProgressInterval
		if interval <= 0 {
			interval = 500 * time.Millisecond
		}
		stopReporting = newReporter(root, trk, interval, opts.Progress)
	}
	// Deferred so the final snapshot is delivered once every row has reached the
	// store, including the last partial batch the consumer flushes below.
	defer stopReporting()

	if opts.CountTotal {
		if err := countEntries(ctx, root, trk); err != nil {
			return err
		}
	}

	// Single full-table read: the snapshot is read-only from here on and is
	// shared between the walk (quick-mode skip checks) and the consumer
	// (insert-vs-update decisions) without locking.
	existing, err := s.LoadExisting()
	if err != nil {
		return fmt.Errorf("load existing rows: %w", err)
	}

	results := make(chan store.FileInfo)
	semCount := make(chan struct{}, workers) // bounds concurrent hasher count
	budget := newByteBudget(maxBytes)        // bounds content resident in those hashers
	var wg sync.WaitGroup

	// Consumer: drain results into the store in batched transactions until the
	// channel is closed or ctx is cancelled. Batching amortizes the transaction
	// overhead (a single commit for many rows is ~200x faster than one commit
	// per row) and bounds how much work a Ctrl-C can cost: at most the in-flight
	// batch.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		batch := make([]store.FileInfo, 0, batchRows)
		isUpdate := make([]bool, 0, batchRows)
		var batchSize int64
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if err := s.WriteBatch(batch, isUpdate); err != nil {
				log.Error().Err(err).Int("rows", len(batch)).Msg("failed to write batch")
			}
			batch = batch[:0]
			isUpdate = isUpdate[:0]
			batchSize = 0
		}
		for {
			select {
			case fi, ok := <-results:
				if !ok {
					// Channel closed, flush remaining and exit
					flush()
					return
				}
				old, ok := existing[fi.Path]
				// Existing row: skip if nothing changed; otherwise UPDATE. New
				// row: INSERT. Deciding here from the read-only snapshot lets us
				// batch writes without a per-row existence lookup.
				if ok {
					if fi.Imohash == old.Imohash && fi.XXH3Hash == old.XXH3Hash {
						continue // Skip unchanged files
					}
					// A result that was never hashed (-no-hash) must not blank the
					// hashes already held for a file whose size and modtime are
					// unchanged, or a single no-hash run would erase the duplicate
					// index for the whole tree.
					if fi.XXH3Hash == "" && old.XXH3Hash != "" &&
						fi.Size == old.Size && fi.ModTime.Equal(old.ModTime) {
						continue
					}
				}
				batch = append(batch, fi)
				isUpdate = append(isUpdate, ok)
				batchSize += fi.Size
				trk.indexed.Add(1)
				if len(batch) >= batchRows || batchSize >= batchBytes {
					flush()
				}
			case <-ctx.Done():
				// Cancellation: flush current batch and exit immediately
				flush()
				return
			}
		}
	}()

	walkErr := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("walk error")
			return nil // skip this entry, keep walking
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		trk.scanned.Add(1)
		fi := store.FileInfo{Path: path, Size: info.Size(), ModTime: info.ModTime()}

		switch classify(existing, path, info, opts) {
		case actionSkip:
			trk.skipped.Add(1)
			return nil
		case actionRecord:
			select {
			case results <- fi:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}

		wg.Add(1)
		// Acquire the count slot (bounded by workers). On cancellation we bail
		// without consuming a slot so the consumer can drain and flush.
		select {
		case semCount <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			return ctx.Err()
		}
		// The charge is what the hash will hold in memory, not the file size: a
		// huge file is streamed, so charging it its full size would serialise the
		// whole run onto one worker.
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

			imo, xxh, hErr := hashFile(path)
			if hErr != nil {
				log.Error().Err(hErr).Str("path", path).Msg("failed to hash file; recording unhashed")
			} else {
				fi.Imohash, fi.XXH3Hash = imo, xxh
				trk.hashed.Add(1)
				trk.bytes.Add(info.Size())
			}
			select {
			case results <- fi:
			case <-ctx.Done():
			}
		}()
		return nil
	})

	wg.Wait()
	close(results)
	<-consumerDone

	// Wake any budget waiters so they can observe the cancellation. (The
	// walk is over, so there are none in practice, but this keeps the condvar
	// honest against future callers of Run on the same machinery.)
	budget.wakeAll()

	if walkErr != nil && !errors.Is(walkErr, context.Canceled) {
		return fmt.Errorf("walk %q: %w", root, walkErr)
	}
	// A cancelled walk is reported as context.Canceled (not nil) so callers can
	// tell an interrupted run from a complete one: the flush above already
	// committed the partial progress either way.
	return walkErr
}

// entryAction says what the walk should do with one directory entry.
type entryAction int

const (
	// actionSkip leaves an entry alone entirely: quick mode has already indexed
	// it, so neither its row nor its stored hashes are rewritten.
	actionSkip entryAction = iota
	// actionRecord stores path, size and modtime without hashing.
	actionRecord
	// actionHash hashes the content before recording it.
	actionHash
)

// classify decides how one directory entry is indexed: quick mode leaves files
// the snapshot already holds untouched, hashing can be switched off entirely
// (-no-hash), and non-regular entries (directories, symlinks) are only ever
// recorded, never hashed. existing is the read-only map returned by
// LoadExisting; it is not mutated here.
func classify(existing map[string]store.FileInfo, path string, info fs.FileInfo, opts Options) entryAction {
	if !info.Mode().IsRegular() {
		return actionRecord
	}
	if opts.Quick {
		if _, seen := existing[path]; seen {
			return actionSkip
		}
	}
	if !opts.Hash {
		return actionRecord
	}
	return actionHash
}
