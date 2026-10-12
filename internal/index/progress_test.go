package index

import (
	"context"
	"errors"
	"testing"
	"time"
)

// buildTree yields six walked entries: the root directory, f1.txt, f2.txt, the
// sub directory, sub/f3.txt, and a symlink. Only the three regular files are
// hashed.
const (
	treeEntries = 6
	treeFiles   = 3
	treeBytes   = int64(2*len("duplicate content") + len("unique content"))
)

func TestRunReportsCounters(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	var snaps []Progress
	opts := Options{Hash: true, Workers: 2, ProgressInterval: time.Millisecond,
		Progress: func(p Progress) { snaps = append(snaps, p) }}
	if err := Run(s, root, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(snaps) == 0 {
		t.Fatal("no progress reported")
	}
	last := snaps[len(snaps)-1]
	if last.Root != root {
		t.Errorf("root = %q, want %q", last.Root, root)
	}
	if last.Scanned != treeEntries {
		t.Errorf("scanned = %d, want %d", last.Scanned, treeEntries)
	}
	if last.Total != 0 {
		t.Errorf("total = %d, want 0 when CountTotal is off", last.Total)
	}
	if last.Counting {
		t.Error("final snapshot still marked as counting")
	}
	if last.Hashed != treeFiles {
		t.Errorf("hashed = %d, want %d", last.Hashed, treeFiles)
	}
	if last.Bytes != treeBytes {
		t.Errorf("bytes = %d, want %d", last.Bytes, treeBytes)
	}
	if last.Indexed != treeEntries {
		t.Errorf("indexed = %d, want %d", last.Indexed, treeEntries)
	}
	if last.Skipped != 0 {
		t.Errorf("skipped = %d, want 0", last.Skipped)
	}
	// Counters only ever move forward, otherwise a redraw can show a run going
	// backwards.
	for i := 1; i < len(snaps); i++ {
		if snaps[i].Scanned < snaps[i-1].Scanned || snaps[i].Indexed < snaps[i-1].Indexed {
			t.Fatalf("snapshot %d went backwards: %+v after %+v", i, snaps[i], snaps[i-1])
		}
	}
}

func TestRunQuickReportsSkips(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)
	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var last Progress
	opts := Options{Hash: true, Quick: true, Workers: 2, ProgressInterval: time.Millisecond,
		Progress: func(p Progress) { last = p }}
	if err := Run(s, root, opts); err != nil {
		t.Fatalf("quick Run: %v", err)
	}

	if last.Skipped != treeFiles {
		t.Errorf("skipped = %d, want %d", last.Skipped, treeFiles)
	}
	if last.Hashed != 0 {
		t.Errorf("hashed = %d, want 0", last.Hashed)
	}
	// Quick mode hands the directories and the symlink to the consumer again,
	// which drops them because their rows are unchanged.
	if last.Indexed != 0 {
		t.Errorf("indexed = %d, want 0", last.Indexed)
	}
}

func TestProgressFinalSnapshotAlwaysDelivered(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	var calls int
	// An interval longer than the run means the only report is the final one.
	opts := Options{Hash: true, Workers: 2, ProgressInterval: time.Hour,
		Progress: func(Progress) { calls++ }}
	if err := Run(s, root, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if calls != 1 {
		t.Fatalf("progress calls = %d, want exactly the final snapshot", calls)
	}
}

func TestProgressNilIsAllowed(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)
	if err := Run(s, root, Options{Hash: true, Workers: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestCountEntriesMatchesIndexedEntries(t *testing.T) {
	s := openStore(t)
	root := buildTree(t)

	var last Progress
	opts := Options{Hash: true, Workers: 2, CountTotal: true, ProgressInterval: time.Millisecond,
		Progress: func(p Progress) { last = p }}
	if err := Run(s, root, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if last.Total != treeEntries {
		t.Errorf("total = %d, want %d", last.Total, treeEntries)
	}
	// The pre-walk and the indexing walk must visit the same set of entries,
	// otherwise a finished run would show 37,000/38,012 and look truncated.
	if last.Total != last.Scanned {
		t.Errorf("total = %d but scanned = %d", last.Total, last.Scanned)
	}
}

func TestCountEntriesCancellation(t *testing.T) {
	root := buildTree(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := countEntries(ctx, root, &tracker{start: time.Now()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("countEntries error = %v, want context.Canceled", err)
	}
}
