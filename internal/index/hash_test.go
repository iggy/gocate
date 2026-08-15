package index

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func TestHashFileDeterministicAndMatching(t *testing.T) {
	dir := t.TempDir()
	a := writeFile(t, dir, "a", "hello world")
	b := writeFile(t, dir, "b", "hello world")

	imoA, xxhA, err := hashFile(a)
	if err != nil {
		t.Fatalf("hashFile a: %v", err)
	}
	if imoA == "" || xxhA == "" {
		t.Fatal("hashFile returned empty hashes for non-empty file")
	}

	imoB, xxhB, err := hashFile(b)
	if err != nil {
		t.Fatalf("hashFile b: %v", err)
	}
	if imoA != imoB || xxhA != xxhB {
		t.Fatalf("identical files hashed differently: imo %q/%q xxh %q/%q", imoA, imoB, xxhA, xxhB)
	}
}

func TestHashFileSkipsEmpty(t *testing.T) {
	dir := t.TempDir()
	empty := writeFile(t, dir, "empty", "")

	imo, xxh, err := hashFile(empty)
	if err != nil {
		t.Fatalf("hashFile empty: %v", err)
	}
	if imo != "" || xxh != "" {
		t.Fatalf("zero-byte file should be skipped, got imo %q xxh %q", imo, xxh)
	}
}

func TestHashFileMissing(t *testing.T) {
	if _, _, err := hashFile(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("hashFile on missing file should error")
	}
}
