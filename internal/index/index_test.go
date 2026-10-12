package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/iggy/gocate/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir(), "testhost")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// buildTree creates: f1.txt, f2.txt (identical to f1), sub/f3.txt, and a symlink.
func buildTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, root, "f1.txt", "duplicate content")
	writeFile(t, root, "f2.txt", "duplicate content")
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	writeFile(t, filepath.Join(root, "sub"), "f3.txt", "unique content")
	if err := os.Symlink(filepath.Join(root, "f1.txt"), filepath.Join(root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	return root
}

func TestRunHashesAndDetectsDuplicates(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// f1 and f2 share content -> one duplicate group.
	groups, err := s.Duplicates()
	if err != nil {
		t.Fatalf("Duplicates: %v", err)
	}
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("got dup groups %+v, want one group of 2", groups)
	}

	// The three regular files should be searchable; the symlink target is the
	// same path, so search by extension finds all .txt files.
	files, err := s.Search(`\.txt$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d .txt rows, want 3: %+v", len(files), files)
	}
}

func TestRunNoHash(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: false}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	files, err := s.Search(`\.txt$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d .txt rows, want 3", len(files))
	}
	for _, f := range files {
		if f.XXH3Hash != "" || f.Imohash != "" {
			t.Fatalf("no-hash run produced hashes for %q", f.Path)
		}
	}
}

func TestRunQuickSkipsExisting(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	// First pass with no hashing: rows exist but unhashed.
	if err := Run(s, root, Options{Hash: false}); err != nil {
		t.Fatalf("Run no-hash: %v", err)
	}
	// Quick pass: existing rows are skipped, so they remain unhashed.
	if err := Run(s, root, Options{Hash: true, Quick: true, Workers: runtime.NumCPU()}); err != nil {
		t.Fatalf("Run quick: %v", err)
	}

	files, err := s.Search(`\.txt$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for _, f := range files {
		if f.XXH3Hash != "" {
			t.Fatalf("quick run re-hashed existing file %q", f.Path)
		}
	}
}

// TestRunByteBudgetContention forces every file to wait on the in-flight byte
// budget by setting it below a single file's size. The budget wait used to run
// with its mutex released, which killed the process with an "unlock of
// unlocked mutex" fatal error as soon as one file exceeded the budget.
func TestRunByteBudgetContention(t *testing.T) {
	s := openStore(t)
	root := t.TempDir()
	want := 4
	for i, name := range []string{"a.bin", "b.bin", "c.bin", "d.bin"} {
		writeFile(t, root, name, strings.Repeat(string(rune('a'+i)), 4096))
	}

	if err := Run(s, root, Options{Hash: true, Workers: 4, MaxBytes: 1}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	files, err := s.Search(`\.bin$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(files) != want {
		t.Fatalf("got %d .bin rows, want %d", len(files), want)
	}
	for _, f := range files {
		if f.XXH3Hash == "" {
			t.Fatalf("file %q was not hashed under the byte budget", f.Path)
		}
	}
}

// TestRunQuickPreservesStoredHashes guards the incremental path: a quick
// re-index must leave existing rows exactly as they were, including their
// hashes, so duplicate detection keeps working across runs.
func TestRunQuickPreservesStoredHashes(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := Run(s, root, Options{Hash: true, Quick: true, Workers: 2}); err != nil {
		t.Fatalf("quick Run: %v", err)
	}

	groups, err := s.Duplicates()
	if err != nil {
		t.Fatalf("Duplicates: %v", err)
	}
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("quick run lost duplicate groups: %+v", groups)
	}
}

// TestRunNoHashPreservesStoredHashes covers the same hazard from the other
// direction: recording metadata without hashing must not erase the hashes of
// files that have not changed since they were hashed.
func TestRunNoHashPreservesStoredHashes(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := Run(s, root, Options{Hash: false}); err != nil {
		t.Fatalf("no-hash Run: %v", err)
	}

	files, err := s.Search(`\.txt$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d .txt rows, want 3", len(files))
	}
	for _, f := range files {
		if f.XXH3Hash == "" || f.Imohash == "" {
			t.Fatalf("no-hash run erased the stored hashes of %q", f.Path)
		}
	}
}

// TestRunReindexIsIdempotent guards against a regression where the batched
// write path inserts duplicate rows instead of updating in place. Indexing the
// same tree a second time must not change the row count.
func TestRunReindexIsIdempotent(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	count := func(t *testing.T) int {
		t.Helper()
		files, err := s.Dump()
		if err != nil {
			t.Fatalf("Dump: %v", err)
		}
		return len(files)
	}

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	afterFirst := count(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := count(t); got != afterFirst {
		t.Fatalf("row count changed after re-index: %d -> %d (duplicate inserts)", afterFirst, got)
	}

	// A third run in quick mode must also be a no-op count-wise.
	if err := Run(s, root, Options{Hash: true, Quick: true, Workers: 2}); err != nil {
		t.Fatalf("quick Run: %v", err)
	}
	if got := count(t); got != afterFirst {
		t.Fatalf("row count changed after quick re-index: %d -> %d", afterFirst, got)
	}
}

// TestRunCtxCancellation checks that an interrupted run says so instead of
// looking like it indexed the whole tree.
func TestRunCtxCancellation(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := RunCtx(ctx, s, root, Options{Hash: true, Workers: 2})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCtx error = %v, want context.Canceled", err)
	}
}
