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
package index

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"

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
	// MaxBytes caps the total size of files being hashed concurrently, on top
	// of the Workers count cap. A huge file consumes most of the budget and so
	// runs nearly alone, while many small files can fill the workers. Values
	// <= 0 default to 256 MiB per worker.
	MaxBytes int64
	// BatchRows is how many rows the consumer accumulates before committing a
	// write transaction. <= 0 defaults to 1000.
	BatchRows int
	// BatchBytes bounds a write batch by the total content size of the files it
	// holds (sum of fi.Size), so a Ctrl-C that drops an in-flight batch never
	// forfeits more than this much hashing effort. <= 0 defaults to 4 GiB.
	BatchBytes int64
}

// Run indexes the tree rooted at root into s according to opts. It is the
// convenience form of RunCtx that cannot be cancelled.
func Run(s *store.Store, root string, opts Options) error {
	return RunCtx(context.Background(), s, root, opts)
}

// RunCtx indexes the tree rooted at root into s according to opts, stopping
// early when ctx is cancelled. A cancelled walk still flushes the in-flight
// write batch so partial progress is committed rather than lost.
func RunCtx(ctx context.Context, s *store.Store, root string, opts Options) error {
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		// Enough room for the workers to each be hashing a small-ish file at
		// once, without letting the budget explode for huge trees.
		maxBytes = int64(workers) * 256 * (1 << 20) // 256 MiB per worker
	}
	batchRows := opts.BatchRows
	if batchRows <= 0 {
		batchRows = 1000
	}
	batchBytes := opts.BatchBytes
	if batchBytes <= 0 {
		batchBytes = 4 << 30 // 4 GiB
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
	var semBytes int64                       // bounds total bytes in flight (guarded by semMu)
	var semMu sync.Mutex
	semCond := sync.NewCond(&semMu)
	var wg sync.WaitGroup

	// Consumer: drain results into the store in batched transactions until the
	// channel is closed. Batching amortizes the transaction overhead (a single
	// commit for many rows is ~200x faster than one commit per row) and bounds
	// how much work a Ctrl-C can cost: at most the in-flight batch.
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
		for fi := range results {
			old, ok := existing[fi.Path]
			// Existing row: skip if nothing changed; otherwise UPDATE. New
			// row: INSERT. Deciding here from the read-only snapshot lets us
			// batch writes without a per-row existence lookup.
			if ok && fi.Imohash == old.Imohash && fi.XXH3Hash == old.XXH3Hash {
				continue
			}
			batch = append(batch, fi)
			isUpdate = append(isUpdate, ok)
			batchSize += fi.Size
			if len(batch) >= batchRows || batchSize >= batchBytes {
				flush()
			}
		}
		flush()
	}()

	// acquireBytes blocks until n bytes of the in-flight budget are free, or
	// until ctx is cancelled. Returns ctx.Err() on cancellation.
	acquireBytes := func(n int64) error {
		semMu.Lock()
		defer semMu.Unlock()
		for semBytes+n > maxBytes {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			semCond.Wait()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		semBytes += n
		return nil
	}
	// releaseByte returns n bytes of the in-flight budget to the pool. It runs
	// in the hasher goroutine after hashing completes.
	releaseByte := func(n int64) {
		semMu.Lock()
		semBytes -= n
		semCond.Signal()
		semMu.Unlock()
	}

	walkErr := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("walk error")
			return nil // skip this entry, keep walking
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fi := store.FileInfo{Path: path, Size: info.Size(), ModTime: info.ModTime()}

		if !shouldHash(existing, path, info, opts) {
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
		// Acquire the byte budget. If cancelled here, release the count slot
		// we just took.
		if err := acquireBytes(info.Size()); err != nil {
			<-semCount
			wg.Done()
			return err
		}
		go func() {
			defer wg.Done()
			defer func() { <-semCount }()
			defer releaseByte(info.Size())

			imo, xxh, hErr := hashFile(path)
			if hErr != nil {
				log.Error().Err(hErr).Str("path", path).Msg("failed to hash file; recording unhashed")
			} else {
				fi.Imohash, fi.XXH3Hash = imo, xxh
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

	// Wake any acquireBytes waiters so they can observe the cancellation. (The
	// walk is over, so there are none in practice, but this keeps the condvar
	// honest against future callers of Run on the same machinery.)
	semMu.Lock()
	semCond.Broadcast()
	semMu.Unlock()

	if walkErr != nil && walkErr != context.Canceled {
		return fmt.Errorf("walk %q: %w", root, walkErr)
	}
	return nil
}

// shouldHash reports whether a dirent should be hashed: it must be a regular
// file, hashing must be enabled, and in quick mode it must not already be in
// the snapshot. existing is the read-only map returned by LoadExisting; it is
// not mutated here.
func shouldHash(existing map[string]store.FileInfo, path string, info fs.FileInfo, opts Options) bool {
	if !opts.Hash || !info.Mode().IsRegular() {
		return false
	}
	if opts.Quick {
		_, has := existing[path]
		return !has
	}
	return true
}
