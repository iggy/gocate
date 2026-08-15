// Package index walks a filesystem tree and records file metadata and content
// hashes into a store.Store.
//
// The pipeline is a bounded producer/consumer fan-out: filepath.Walk produces
// dirents, a worker pool hashes regular files concurrently (capped so a large
// tree cannot exhaust file descriptors), and a single consumer goroutine writes
// every result to the store.
package index

import (
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
}

// Run indexes the tree rooted at root into s according to opts.
func Run(s *store.Store, root string, opts Options) error {
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	results := make(chan store.FileInfo)
	sem := make(chan struct{}, workers) // bounds concurrent hashers
	var wg sync.WaitGroup

	// Consumer: drain results into the store until the channel is closed.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for fi := range results {
			if err := s.Upsert(fi, opts.Quick); err != nil {
				log.Error().Err(err).Str("path", fi.Path).Msg("failed to upsert file")
			}
		}
	}()

	walkErr := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("walk error")
			return nil // skip this entry, keep walking
		}

		fi := store.FileInfo{Path: path, Size: info.Size(), ModTime: info.ModTime()}

		if !shouldHash(s, path, info, opts) {
			results <- fi
			return nil
		}

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			imo, xxh, err := hashFile(path)
			if err != nil {
				log.Error().Err(err).Str("path", path).Msg("failed to hash file; recording unhashed")
			} else {
				fi.Imohash, fi.XXH3Hash = imo, xxh
			}
			results <- fi
		}()
		return nil
	})

	wg.Wait()
	close(results)
	<-consumerDone

	if walkErr != nil {
		return fmt.Errorf("walk %q: %w", root, walkErr)
	}
	return nil
}

// shouldHash reports whether a dirent should be hashed: it must be a regular
// file, hashing must be enabled, and in quick mode it must not already be in
// the store.
func shouldHash(s *store.Store, path string, info fs.FileInfo, opts Options) bool {
	if !opts.Hash || !info.Mode().IsRegular() {
		return false
	}
	if opts.Quick {
		has, err := s.Has(path)
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("quick existence check failed; hashing anyway")
			return true
		}
		return !has
	}
	return true
}
