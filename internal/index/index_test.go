package index

import (
	"os"
	"path/filepath"
	"runtime"
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
