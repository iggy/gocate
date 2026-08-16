package index

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalafut/imohash"
	"github.com/zeebo/xxh3"
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

// TestHashFileMatchesReadFileBasis verifies the streaming implementation
// produces byte-identical hashes to the previous os.ReadFile-based approach
// across small, medium (below imohash sample threshold), and large (above it)
// files, so existing database rows remain valid.
func TestHashFileMatchesReadFileBasis(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"small", []byte("hello world")},
		{"medium", randomBytes(64 * 1024)}, // below imohash.SampleThreshold (128KiB)
		{"large", randomBytes(512 * 1024)}, // above imohash.SampleThreshold
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := writeFile(t, dir, "f", string(tc.data))

			gotImo, gotXxh, err := hashFile(p)
			if err != nil {
				t.Fatalf("hashFile: %v", err)
			}

			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			wantImo := fmt.Sprintf("%x", imohash.Sum(data))
			wantXxh := fmt.Sprintf("%x", xxh3.Hash(data))

			if gotImo != wantImo {
				t.Fatalf("imohash mismatch: got %q want %q", gotImo, wantImo)
			}
			if gotXxh != wantXxh {
				t.Fatalf("xxh3 mismatch: got %q want %q", gotXxh, wantXxh)
			}
		})
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 31)
	}
	return b
}
