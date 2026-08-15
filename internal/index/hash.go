package index

import (
	"fmt"
	"os"

	"github.com/kalafut/imohash"
	"github.com/zeebo/xxh3"
)

// hashFile reads path once and returns its imohash and xxh3 hashes. Zero-byte
// files are skipped (both hashes empty, nil error) since there is nothing to
// distinguish them by content.
func hashFile(path string) (imo, xxh string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("read %q: %w", path, err)
	}
	if len(data) == 0 {
		return "", "", nil
	}
	return fmt.Sprintf("%x", imohash.Sum(data)), fmt.Sprintf("%x", xxh3.Hash(data)), nil
}
