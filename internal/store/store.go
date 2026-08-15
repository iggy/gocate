// Package store provides persistent storage for gocate's file index.
//
// It wraps an embedded modernc.org/ql database holding a single "files" table
// keyed conceptually by (hostname, filename). Callers get and put FileInfo
// values; all SQL and result-set handling stays inside this package.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"modernc.org/ql"
)

// FileInfo describes one indexed file.
type FileInfo struct {
	Path     string
	Size     int64
	ModTime  time.Time
	Imohash  string // primary hash: fast, samples the file, can collide
	XXH3Hash string // full-content hash: treated as collision-free, used for dupes
}

// Store is a handle to the file index database. Its methods are safe for
// concurrent use: a mutex serializes access to the underlying ql database,
// which lets the indexer run quick existence checks on the walk goroutine while
// the consumer goroutine writes.
type Store struct {
	db       *ql.DB
	ctx      *ql.TCtx
	hostname string

	insertQ ql.List
	selectQ ql.List
	updateQ ql.List

	mu sync.Mutex
}

// Open opens (creating if needed) the file index database under dir. If
// hostname is empty it is resolved from os.Hostname, falling back to "unknown".
func Open(dir, hostname string) (*Store, error) {
	if hostname == "" {
		hn, err := os.Hostname()
		if err != nil {
			hn = "unknown"
		}
		hostname = hn
	}

	if err := os.MkdirAll(dir, 0o775); err != nil {
		return nil, fmt.Errorf("create config dir %q: %w", dir, err)
	}

	dbFile := filepath.Join(dir, "files.db")
	db, err := ql.OpenFile(dbFile, &ql.Options{CanCreate: true, FileFormat: 2})
	if err != nil {
		return nil, fmt.Errorf("open db %q: %w", dbFile, err)
	}

	s := &Store{db: db, ctx: ql.NewRWCtx(), hostname: hostname}

	if _, _, err := db.Run(s.ctx, `
		BEGIN TRANSACTION;
			CREATE TABLE IF NOT EXISTS files (
				hostname string,
				filename string,
				size int64,
				modtimestamp time,
				imohash string,
				xxh3hash string,
			);
		COMMIT;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}

	if err := s.compileQueries(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

// compileQueries precompiles the per-row statements used during indexing. The
// hostname is embedded as a literal: it is derived from the host, never from
// untrusted search input.
func (s *Store) compileQueries() error {
	var err error

	if s.insertQ, err = ql.Compile(fmt.Sprintf(`
		BEGIN TRANSACTION;
			INSERT INTO files VALUES("%s", $1, $2, $3, $4, $5);
		COMMIT;`, s.hostname)); err != nil {
		return fmt.Errorf("compile insert: %w", err)
	}

	if s.selectQ, err = ql.Compile(fmt.Sprintf(`
		SELECT * FROM files WHERE hostname == "%s" && filename == $1;`, s.hostname)); err != nil {
		return fmt.Errorf("compile select: %w", err)
	}

	if s.updateQ, err = ql.Compile(fmt.Sprintf(`
		BEGIN TRANSACTION;
			UPDATE files SET
				hostname = "%s",
				size = $2,
				modtimestamp = $3,
				imohash = $4,
				xxh3hash = $5
			WHERE filename = $1;
		COMMIT;`, s.hostname)); err != nil {
		return fmt.Errorf("compile update: %w", err)
	}

	return nil
}

// Close flushes and closes the underlying database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("flush db: %w", err)
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}
	return nil
}

// Has reports whether a row already exists for path on this host. It is used by
// quick (incremental) indexing to skip files already recorded.
func (s *Store) Has(path string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, err := s.firstRow(path)
	if err != nil {
		return false, err
	}
	return len(fr) != 0, nil
}

// firstRow returns the existing row for path (empty if none). Callers must hold s.mu.
func (s *Store) firstRow(path string) ([]any, error) {
	rs, _, err := s.db.Execute(s.ctx, s.selectQ, path)
	if err != nil {
		return nil, fmt.Errorf("select %q: %w", path, err)
	}
	fr, err := rs[0].FirstRow()
	if err != nil {
		return nil, fmt.Errorf("first row %q: %w", path, err)
	}
	return fr, nil
}

// Upsert inserts fi if no row exists for its path, otherwise updates the row
// when a hash has changed. When quick is true, existing rows are left untouched.
func (s *Store) Upsert(fi FileInfo, quick bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, err := s.firstRow(fi.Path)
	if err != nil {
		return err
	}

	// No existing row: insert.
	if len(fr) == 0 {
		if _, _, err := s.db.Execute(s.ctx, s.insertQ,
			fi.Path, fi.Size, fi.ModTime, fi.Imohash, fi.XXH3Hash); err != nil {
			return fmt.Errorf("insert %q: %w", fi.Path, err)
		}
		return nil
	}

	// Existing row: in quick mode leave it alone; otherwise update if a hash changed.
	// Columns: 0 hostname, 1 filename, 2 size, 3 modtimestamp, 4 imohash, 5 xxh3hash.
	if quick {
		return nil
	}
	if fr[4] != fi.Imohash || fr[5] != fi.XXH3Hash {
		if _, _, err := s.db.Execute(s.ctx, s.updateQ,
			fi.Path, fi.Size, fi.ModTime, fi.Imohash, fi.XXH3Hash); err != nil {
			return fmt.Errorf("update %q: %w", fi.Path, err)
		}
	}
	return nil
}

// Search returns files whose filename matches the given pattern. The pattern is
// a regular expression: ql's LIKE operator is regex-based, not SQL globbing.
func (s *Store) Search(pattern string) ([]FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rss, _, err := s.db.Run(s.ctx, "SELECT * FROM files WHERE filename LIKE $1;", pattern)
	if err != nil {
		return nil, fmt.Errorf("search %q: %w", pattern, err)
	}
	return collectFiles(rss)
}

// Dump returns every row in the index.
func (s *Store) Dump() ([]FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rss, _, err := s.db.Run(s.ctx, "SELECT * FROM files;")
	if err != nil {
		return nil, fmt.Errorf("dump: %w", err)
	}
	return collectFiles(rss)
}

// Duplicates returns groups of filenames that share an xxh3 content hash. Only
// groups with more than one file are returned. Files with an empty hash (e.g.
// indexed with -no-hash, or zero-byte) are ignored.
func (s *Store) Duplicates() ([][]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rss, _, err := s.db.Run(s.ctx, "SELECT filename, xxh3hash FROM files;")
	if err != nil {
		return nil, fmt.Errorf("select for dupes: %w", err)
	}

	byHash := make(map[string][]string)
	for _, rs := range rss {
		if err := rs.Do(false, func(data []any) (bool, error) {
			filename, _ := data[0].(string)
			hash, _ := data[1].(string)
			if hash == "" {
				return true, nil
			}
			byHash[hash] = append(byHash[hash], filename)
			return true, nil
		}); err != nil {
			return nil, fmt.Errorf("iterate dupes: %w", err)
		}
	}

	var groups [][]string
	for _, files := range byHash {
		if len(files) > 1 {
			sort.Strings(files)
			groups = append(groups, files)
		}
	}
	// Deterministic group order so script output is stable across runs.
	sort.Slice(groups, func(i, j int) bool {
		return groups[i][0] < groups[j][0]
	})
	return groups, nil
}

// Info returns the database name and the list of table names.
func (s *Store) Info() (name string, tables []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.db.Info()
	if err != nil {
		return "", nil, fmt.Errorf("db info: %w", err)
	}
	for _, t := range info.Tables {
		tables = append(tables, t.Name)
	}
	return info.Name, tables, nil
}

// collectFiles materializes "SELECT *" result sets into FileInfo values.
// Columns: 0 hostname, 1 filename, 2 size, 3 modtimestamp, 4 imohash, 5 xxh3hash.
func collectFiles(rss []ql.Recordset) ([]FileInfo, error) {
	var out []FileInfo
	for _, rs := range rss {
		if err := rs.Do(false, func(data []any) (bool, error) {
			fi := FileInfo{}
			fi.Path, _ = data[1].(string)
			fi.Size, _ = data[2].(int64)
			fi.ModTime, _ = data[3].(time.Time)
			fi.Imohash, _ = data[4].(string)
			fi.XXH3Hash, _ = data[5].(string)
			out = append(out, fi)
			return true, nil
		}); err != nil {
			return nil, fmt.Errorf("iterate rows: %w", err)
		}
	}
	return out, nil
}
