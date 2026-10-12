package index

import (
	"fmt"
	"io"
	"os"

	"github.com/kalafut/imohash"
	"github.com/zeebo/xxh3"
)

// hashBufferLimit is the largest file contentHasher reads in one pass. At or
// below this size imohash hashes the whole file anyway, so buffering it turns
// two reads (samples plus the full pass) into one, while peak memory stays at
// one buffer per hashing worker.
const hashBufferLimit = imohash.SampleThreshold

// streamReadBuffer is the buffer a hash of a file larger than hashBufferLimit
// reads through, so hashing a huge file costs this and not the file.
const streamReadBuffer = 256 << 10

// contentHasher computes the two content hashes of one open file and reads only
// what the caller asks for. Files up to hashBufferLimit are buffered so both
// hashes come from a single read; larger files are never buffered, imohash reads
// its fixed samples and xxh3 streams the contents through a fixed buffer.
type contentHasher struct {
	path string
	f    *os.File
	sr   *io.SectionReader
	size int64
	buf  []byte
}

// openContentHasher opens path for hashing.
func openContentHasher(path string) (*contentHasher, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}

	h := &contentHasher{path: path, f: f, size: fi.Size()}
	if h.size > 0 && h.size <= hashBufferLimit {
		h.buf = make([]byte, h.size)
		if _, err := io.ReadFull(io.NewSectionReader(f, 0, h.size), h.buf); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("read %q: %w", path, err)
		}
		return h, nil
	}
	h.sr = io.NewSectionReader(f, 0, h.size)
	return h, nil
}

// Close releases the underlying file. It is safe to call more than once.
func (h *contentHasher) Close() error {
	if h.f == nil {
		return nil
	}
	f := h.f
	h.f = nil
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %q: %w", h.path, err)
	}
	return nil
}

// sample returns the imohash of the file: a fast hash over the file size and a
// fixed number of sample bytes, so its cost barely depends on file size. Two
// different files can share a sample, so matching hashes only make equality
// likely, while differing hashes prove the contents differ.
func (h *contentHasher) sample() (string, error) {
	if h.size == 0 {
		return "", nil
	}
	if h.buf != nil {
		return fmt.Sprintf("%x", imohash.Sum(h.buf)), nil
	}

	imoSum, err := imohash.SumSectionReader(h.sr)
	if err != nil {
		return "", fmt.Errorf("imohash %q: %w", h.path, err)
	}
	return fmt.Sprintf("%x", imoSum), nil
}

// full returns the xxh3 hash of the entire file, streamed so the contents are
// never held in memory at once. It is treated as collision-free and is what
// duplicate detection relies on.
//
// sample leaves the section reader positioned somewhere inside the file, so the
// stream has to start with a rewind.
func (h *contentHasher) full() (string, error) {
	if h.size == 0 {
		return "", nil
	}
	if h.buf != nil {
		return fmt.Sprintf("%x", xxh3.Hash(h.buf)), nil
	}

	if _, err := h.sr.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek %q: %w", h.path, err)
	}
	sum := xxh3.New()
	buf := make([]byte, streamReadBuffer)
	if _, err := io.CopyBuffer(sum, h.sr, buf); err != nil {
		return "", fmt.Errorf("xxh3 %q: %w", h.path, err)
	}
	return fmt.Sprintf("%x", sum.Sum64()), nil
}

// hashFile returns the imohash and xxh3 hashes of path without reading the whole
// file into memory (unless it is small, see hashBufferLimit). Zero-byte files
// are skipped (both hashes empty, nil error) since there is nothing to
// distinguish them by content.
//
// The hashes are byte-identical to the previous os.ReadFile-based
// implementation, so existing database rows remain valid.
func hashFile(path string) (imo, xxh string, err error) {
	h, err := openContentHasher(path)
	if err != nil {
		return "", "", err
	}
	defer func() {
		if cerr := h.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if imo, err = h.sample(); err != nil {
		return "", "", err
	}
	if xxh, err = h.full(); err != nil {
		return "", "", err
	}
	return imo, xxh, nil
}
