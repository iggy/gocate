// Command gocate is a locate replacement: it walks a filesystem tree, stores
// file metadata and content hashes in an embedded SQL database, and searches
// files by name or lists duplicates by content hash.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/iggy/gocate/internal/index"
	"github.com/iggy/gocate/internal/store"
)

var (
	updatedbFlag = flag.Bool("updatedb", false, "update the database")
	configDir    = flag.String("config", filepath.Join(os.Getenv("HOME"), ".gocate"), "directory to store the file DB")
	updatePath   = flag.String("path", ".", "path to walk and index (with -updatedb)")
	printDupes   = flag.Bool("dupes", false, "print groups of duplicate files (by content hash)")
	dupesScript  = flag.Bool("dupes-script", false, "like -dupes but emit a shell script that replaces each duplicate with a hardlink to a canonical original")
	showStats    = flag.Bool("stats", false, "print DB stats and dump all rows")
	quick        = flag.Bool("quick", false, "quick update: skip files already in the database")
	noHash       = flag.Bool("no-hash", false, "don't hash files, just record path/size/modtime")
	workersFlag  = flag.Int("workers", 0, "files to hash at once (default: one per CPU; raise it when disk reads, not hashing, are the limit)")
	hostname     = flag.String("hostname", "", "custom hostname to use for the database")
	profile      = flag.Bool("profile", false, "write a CPU profile to default.pgo")
	progressFlag = flag.Bool("progress", true, "show indexing progress on stderr when it is a terminal (set it explicitly to force it on or off)")
	totalFlag    = flag.Bool("total", true, "walk the tree once up front so progress can report a total entry count (extra readdir pass, no file reads)")
)

func main() {
	if err := run(); err != nil {
		log.Error().Err(err).Msg("fatal error")
		os.Exit(1)
	}
}

func run() error {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.WarnLevel)

	flag.Parse()

	// A SIGINT cancels the in-flight indexing run so the consumer can flush its
	// current write batch (committing partial progress) instead of being killed
	// mid-transaction. A second SIGINT forces an immediate exit.
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		first := <-sigCh
		log.Warn().Stringer("signal", first).Msg("interrupt received; waiting for workers to finish, flushing index, and exiting")
		stop()
		if <-sigCh != nil {
			os.Exit(130) // second interrupt: bail out now
		}
	}()

	if *profile {
		stop, err := startProfile("default.pgo")
		if err != nil {
			return err
		}
		defer stop()
	}

	s, err := store.Open(*configDir, *hostname)
	if err != nil {
		return err
	}
	defer func() {
		if err := s.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close db")
		}
	}()

	if *updatedbFlag {
		root, err := filepath.Abs(*updatePath)
		if err != nil {
			return fmt.Errorf("resolve path %q: %w", *updatePath, err)
		}
		var prog *progress
		if progressEnabled() {
			prog = &progress{w: os.Stderr}
		}
		opts := index.Options{Hash: !*noHash, Quick: *quick, Workers: *workersFlag}
		if prog != nil {
			opts.Progress = prog.update
			opts.CountTotal = *totalFlag
		}
		err = index.RunCtx(ctx, s, root, opts)
		switch {
		case err == nil:
			prog.finish("Indexed")
		case errors.Is(err, context.Canceled):
			prog.finish("Interrupted")
			// The summary line above already says the same thing, so only an
			// unattended run needs to be told through the log.
			if prog == nil {
				log.Warn().Msg("indexing interrupted; partial progress committed")
			}
		default:
			prog.finish("Indexing failed")
			return err
		}
	}

	if *printDupes {
		if err := showDuplicates(s); err != nil {
			return err
		}
	}

	if *dupesScript {
		if err := showDuplicatesScript(s); err != nil {
			return err
		}
	}

	if *showStats {
		if err := showInfo(s); err != nil {
			return err
		}
	}

	if flag.NArg() > 0 {
		if err := search(s, flag.Arg(0)); err != nil {
			return err
		}
	}

	return nil
}

// startProfile begins CPU profiling, returning a stop function to defer.
func startProfile(path string) (func(), error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create CPU profile: %w", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start CPU profile: %w", err)
	}
	return func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close profile")
		}
	}, nil
}

func search(s *store.Store, pattern string) error {
	files, err := s.Search(pattern)
	if err != nil {
		return err
	}
	for _, f := range files {
		fmt.Println(f.Path)
	}
	return nil
}

func showDuplicates(s *store.Store) error {
	groups, err := s.Duplicates()
	if err != nil {
		return err
	}
	for _, files := range groups {
		fmt.Println(strings.Join(files, " "))
	}
	return nil
}

// showDuplicatesScript prints a /bin/sh script that deduplicates files by
// replacing every copy in a duplicate group with a hardlink to the first
// (canonical) member of the group. The canonical file is left untouched, so no
// content is lost and only one inode's worth of disk is consumed per group.
// The first member of each group is chosen deterministically (sorted), so
// re-running the script after more files have been added is stable.
func showDuplicatesScript(s *store.Store) error {
	groups, err := s.Duplicates()
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return nil
	}
	fmt.Println("#!/bin/sh")
	fmt.Println("# Auto-generated by `gocate -dupes-script`. Replaces each duplicate")
	fmt.Println("# with a hardlink to the first path in its group. Review before running.")
	fmt.Println("set -eu")
	for _, files := range groups {
		canonical := files[0]
		for _, dup := range files[1:] {
			if dup == canonical {
				continue
			}
			fmt.Printf("ln -f -- %s %s\n", shellQuote(canonical), shellQuote(dup))
		}
	}
	return nil
}

// shellQuote wraps a path in single quotes, escaping any embedded single
// quotes so the result is safe to interpolate into a /bin/sh script.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func showInfo(s *store.Store) error {
	name, tables, err := s.Info()
	if err != nil {
		return err
	}
	fmt.Printf("db: %s tables: %s\n", name, strings.Join(tables, ", "))

	files, err := s.Dump()
	if err != nil {
		return err
	}
	for _, f := range files {
		fmt.Printf("%s\t%d\t%s\t%s\t%s\n", f.Path, f.Size, f.ModTime.Format("2006-01-02 15:04:05"), f.Imohash, f.XXH3Hash)
	}
	return nil
}
