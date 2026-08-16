package index

import (
	"fmt"
	"io"
	"os"

	"github.com/kalafut/imohash"
	"github.com/zeebo/xxh3"
)

// hashFile returns the imohash and xxh3 hashes of path without reading the
// whole file into memory. imohash only samples a fixed number of bytes from
// the file (see imohash.SampleSize/SampleThreshold) and xxh3 is computed by
// streaming the file in chunks, so peak memory stays bounded regardless of
// file size. Zero-byte files are skipped (both hashes empty, nil error) since
// there is nothing to distinguish them by content.
//
// The hashes are byte-identical to the previous os.ReadFile-based
// implementation, so existing database rows remain valid.
func hashFile(path string) (imo, xxh string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("open %q: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close %q: %w", path, cerr)
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return "", "", fmt.Errorf("stat %q: %w", path, err)
	}
	if fi.Size() == 0 {
		return "", "", nil
	}

	sr := io.NewSectionReader(f, 0, fi.Size())

	// imohash reads only its fixed samples from the section reader. Note that
	// this leaves sr positioned somewhere in the file (the end for small files,
	// the tail sample for large ones), so we must rewind before the next pass.
	imoSum, err := imohash.SumSectionReader(sr)
	if err != nil {
		return "", "", fmt.Errorf("imohash %q: %w", path, err)
	}

	// Rewind to the start and stream xxh3 over the full file so the contents
	// are never held in memory at once.
	if _, err := sr.Seek(0, io.SeekStart); err != nil {
		return "", "", fmt.Errorf("seek %q: %w", path, err)
	}
	h := xxh3.New()
	if _, err := io.Copy(h, sr); err != nil {
		return "", "", fmt.Errorf("xxh3 %q: %w", path, err)
	}

	return fmt.Sprintf("%x", imoSum), fmt.Sprintf("%x", h.Sum64()), nil
}
