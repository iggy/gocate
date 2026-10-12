package index

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/xxh3"

	"github.com/iggy/gocate/internal/store"
)

func TestVerifyCleanTreeReportsNothing(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	drift, err := Verify(s, root)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(drift) != 0 {
		t.Fatalf("clean tree reported drift: %+v", drift)
	}
}

func TestVerifyReportsModifiedAndUnindexed(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	writeFile(t, root, "f1.txt", "changed content")
	writeFile(t, root, "new.txt", "never indexed")

	drift, err := Verify(s, root)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	want := []VerifyDrift{
		{Path: filepath.Join(root, "f1.txt"), Status: StatusModified},
		{Path: filepath.Join(root, "new.txt"), Status: StatusUnindexed},
	}
	if len(drift) != len(want) {
		t.Fatalf("got %+v, want %+v", drift, want)
	}
	for i := range want {
		if drift[i] != want[i] {
			t.Fatalf("drift[%d] = %+v, want %+v", i, drift[i], want[i])
		}
	}
}

// TestVerifyUnreadableCountsAsModified covers the case where a file exists but
// cannot be hashed: the stored hashes can be neither confirmed nor contradicted,
// so the file must be reported rather than silently treated as unchanged.
func TestVerifyUnreadableCountsAsModified(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: file modes do not restrict reads")
	}

	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	target := filepath.Join(root, "sub", "f3.txt")
	if err := os.Chmod(target, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(target, 0o644) })

	drift, err := Verify(s, root)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(drift) != 1 || drift[0].Path != target || drift[0].Status != StatusModified {
		t.Fatalf("got %+v, want one modified entry for %q", drift, target)
	}
}

// TestVerifyNoHashRowsAreModified mirrors -no-hash indexing: the index holds no
// hashes for those rows, so every file must be reported until it is hashed.
func TestVerifyNoHashRowsAreModified(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	if err := Run(s, root, Options{Hash: false}); err != nil {
		t.Fatalf("Run no-hash: %v", err)
	}

	drift, err := Verify(s, root)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(drift) != 3 {
		t.Fatalf("got %d drift entries, want 3 (one per .txt file): %+v", len(drift), drift)
	}
	for _, d := range drift {
		if d.Status != StatusModified {
			t.Fatalf("got %+v, want status modified", d)
		}
	}
}

func TestCompareContentsMatching(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "f", string(randomBytes(300*1024)))

	imo, xxh, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile: %v", err)
	}

	status, err := compareContents(path, store.FileInfo{Path: path, Imohash: imo, XXH3Hash: xxh})
	if err != nil {
		t.Fatalf("compareContents: %v", err)
	}
	if status != StatusMatching {
		t.Fatalf("got %v, want matching", status)
	}
}

// TestCompareContentsCatchesUnsampledChange pins why confirming equality needs
// a full pass: an edit that leaves the size and imohash's sample windows alone
// is invisible to imohash, so only the full-content hash reports it.
func TestCompareContentsCatchesUnsampledChange(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "f", string(randomBytes(512*1024)))

	imo, _, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	storedXXH := xxhOf(t, data)

	edited := append([]byte(nil), data...)
	edited[len(edited)/4] ^= 0xff // outside imohash's head, middle and tail samples
	if err := os.WriteFile(path, edited, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	freshImo, _, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile after edit: %v", err)
	}
	if freshImo != imo {
		t.Fatalf("expected imohash to miss an edit outside its sample windows")
	}

	status, err := compareContents(path, store.FileInfo{Path: path, Imohash: imo, XXH3Hash: storedXXH})
	if err != nil {
		t.Fatalf("compareContents: %v", err)
	}
	if status != StatusModified {
		t.Fatalf("got %v, want modified", status)
	}
}

// TestCompareContentsMissingHashes covers rows that hold no content hash: rows
// written by -no-hash and rows written when a file could not be read. Neither
// can be confirmed, so both are drift.
func TestCompareContentsMissingHashes(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "f", "some content")

	status, err := compareContents(path, store.FileInfo{Path: path})
	if err != nil {
		t.Fatalf("compareContents: %v", err)
	}
	if status != StatusModified {
		t.Fatalf("no-hash row: got %v, want modified", status)
	}

	imo, _, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile: %v", err)
	}
	status, err = compareContents(path, store.FileInfo{Path: path, Imohash: imo})
	if err != nil {
		t.Fatalf("compareContents: %v", err)
	}
	if status != StatusModified {
		t.Fatalf("row without an xxh3: got %v, want modified", status)
	}
}

func TestCompareContentsEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "empty", "")

	status, err := compareContents(path, store.FileInfo{Path: path})
	if err != nil {
		t.Fatalf("compareContents: %v", err)
	}
	if status != StatusMatching {
		t.Fatalf("empty file against an empty row: got %v, want matching", status)
	}
}

func TestCompareContentsUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: file modes do not restrict reads")
	}

	dir := t.TempDir()
	path := writeFile(t, dir, "f", "content")
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	status, err := compareContents(path, store.FileInfo{Path: path, Imohash: "x", XXH3Hash: "y"})
	if err == nil {
		t.Fatal("compareContents on an unreadable file should report an error")
	}
	if status != StatusModified {
		t.Fatalf("got %v, want modified", status)
	}
}

// xxhOf is the reference full-content hash, used to build stored rows without
// going through the code under test.
func xxhOf(t *testing.T, data []byte) string {
	t.Helper()
	return fmt.Sprintf("%x", xxh3.Hash(data))
}
