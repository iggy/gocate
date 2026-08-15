package store

import (
	"sort"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), "testhost")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func TestUpsertInsertAndSearch(t *testing.T) {
	s := openTest(t)

	fi := FileInfo{Path: "/tmp/notes.md", Size: 10, ModTime: time.Unix(1, 0), Imohash: "aa", XXH3Hash: "bb"}
	if err := s.Upsert(fi, false); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := s.Search(`\.md$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 1 || got[0].Path != fi.Path {
		t.Fatalf("Search returned %+v, want one row for %q", got, fi.Path)
	}

	none, err := s.Search(`\.txt$`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("Search for .txt returned %+v, want none", none)
	}
}

func TestUpsertUpdatesOnHashChange(t *testing.T) {
	s := openTest(t)

	fi := FileInfo{Path: "/tmp/f", Size: 1, ModTime: time.Unix(1, 0), Imohash: "old", XXH3Hash: "old"}
	if err := s.Upsert(fi, false); err != nil {
		t.Fatalf("Upsert insert: %v", err)
	}

	fi.Imohash, fi.XXH3Hash = "new", "new"
	if err := s.Upsert(fi, false); err != nil {
		t.Fatalf("Upsert update: %v", err)
	}

	got, err := s.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Dump returned %d rows, want 1 (update must not insert a duplicate)", len(got))
	}
	if got[0].XXH3Hash != "new" {
		t.Fatalf("hash = %q, want updated value %q", got[0].XXH3Hash, "new")
	}
}

func TestUpsertQuickSkipsExisting(t *testing.T) {
	s := openTest(t)

	fi := FileInfo{Path: "/tmp/f", Size: 1, ModTime: time.Unix(1, 0), Imohash: "old", XXH3Hash: "old"}
	if err := s.Upsert(fi, false); err != nil {
		t.Fatalf("Upsert insert: %v", err)
	}

	// Quick mode should leave the existing row untouched even if hashes differ.
	fi.Imohash, fi.XXH3Hash = "new", "new"
	if err := s.Upsert(fi, true); err != nil {
		t.Fatalf("Upsert quick: %v", err)
	}

	got, err := s.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	if len(got) != 1 || got[0].XXH3Hash != "old" {
		t.Fatalf("quick upsert changed row: %+v, want unchanged hash %q", got, "old")
	}
}

func TestHas(t *testing.T) {
	s := openTest(t)

	has, err := s.Has("/tmp/missing")
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if has {
		t.Fatal("Has reported a missing file as present")
	}

	if err := s.Upsert(FileInfo{Path: "/tmp/present", ModTime: time.Unix(1, 0)}, false); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	has, err = s.Has("/tmp/present")
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Fatal("Has reported an inserted file as missing")
	}
}

func TestDuplicates(t *testing.T) {
	s := openTest(t)

	rows := []FileInfo{
		{Path: "/a", ModTime: time.Unix(1, 0), XXH3Hash: "dup"},
		{Path: "/b", ModTime: time.Unix(1, 0), XXH3Hash: "dup"},
		{Path: "/c", ModTime: time.Unix(1, 0), XXH3Hash: "unique"},
		{Path: "/empty", ModTime: time.Unix(1, 0), XXH3Hash: ""}, // unhashed: ignored
	}
	for _, r := range rows {
		if err := s.Upsert(r, false); err != nil {
			t.Fatalf("Upsert %s: %v", r.Path, err)
		}
	}

	groups, err := s.Duplicates()
	if err != nil {
		t.Fatalf("Duplicates: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("got %d dup groups, want 1: %+v", len(groups), groups)
	}
	got := append([]string(nil), groups[0]...)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "/a" || got[1] != "/b" {
		t.Fatalf("dup group = %v, want [/a /b]", got)
	}
}
