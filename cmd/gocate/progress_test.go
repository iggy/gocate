package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/iggy/gocate/internal/index"
)

func TestProgressUpdateRedrawsOneLine(t *testing.T) {
	var buf bytes.Buffer
	p := &progress{w: &buf}

	p.update(index.Progress{Root: "/data", Scanned: 1234567, Indexed: 1234, Bytes: 3<<30 + 1<<29, Elapsed: 90 * time.Second})
	p.update(index.Progress{Root: "/data", Scanned: 2345678, Indexed: 5678, Bytes: 4 << 30, Elapsed: 2 * time.Minute})

	got := buf.String()
	if strings.Contains(got, "\n") {
		t.Errorf("live updates must not emit newlines, got %q", got)
	}
	if n := strings.Count(got, "\r\x1b[K"); n != 2 {
		t.Errorf("redraws = %d, want 2 in %q", n, got)
	}
	for _, want := range []string{"/data", "2,345,678", "5,678", "4.0 GiB", "2m0s"} {
		if !strings.Contains(got, want) {
			t.Errorf("status line %q missing %q", got, want)
		}
	}
}

func TestProgressShowsTotalWhenKnown(t *testing.T) {
	var buf bytes.Buffer
	p := &progress{w: &buf}

	p.update(index.Progress{Root: "/tv", Scanned: 24004, Total: 38012, Elapsed: time.Second})

	got := buf.String()
	if !strings.Contains(got, "24,004/38,012 scanned") {
		t.Errorf("status line %q should show done/total", got)
	}
	p.finish("Indexed")
	if summary := buf.String(); !strings.Contains(summary, "24,004/38,012 entries scanned") {
		t.Errorf("summary %q should keep the total", summary)
	}
}

func TestProgressOmitsUnknownTotal(t *testing.T) {
	var buf bytes.Buffer
	p := &progress{w: &buf}

	p.update(index.Progress{Root: "/tv", Scanned: 24004, Elapsed: time.Second})

	got := buf.String()
	if !strings.Contains(got, "24,004 scanned") {
		t.Errorf("status line %q should show a bare count", got)
	}
	if strings.Contains(got, "/38,012") {
		t.Errorf("status line %q should not invent a total", got)
	}
}

func TestProgressTotalNeverUndercountsScanned(t *testing.T) {
	var buf bytes.Buffer
	p := &progress{w: &buf}

	// Files appearing after the counting pass must not push the run past its own
	// total.
	p.update(index.Progress{Root: "/tv", Scanned: 40010, Total: 40000, Elapsed: time.Second})

	if got := buf.String(); !strings.Contains(got, "40,010/40,010 scanned") {
		t.Errorf("status line %q should cap the total at what was scanned", got)
	}
}

func TestProgressCountingPhase(t *testing.T) {
	var buf bytes.Buffer
	p := &progress{w: &buf}

	p.update(index.Progress{Root: "/tv", Scanned: 1200, Counting: true, Elapsed: 3 * time.Second})

	got := buf.String()
	if !strings.Contains(got, "counting 1,200 entries so far") {
		t.Errorf("counting phase %q should say what it is doing", got)
	}
	if strings.Contains(got, "indexed") || strings.Contains(got, "hashed") {
		t.Errorf("counting phase %q should not report indexing results", got)
	}
}

func TestProgressFinishWritesSummary(t *testing.T) {
	var buf bytes.Buffer
	p := &progress{w: &buf}
	p.update(index.Progress{Root: "/music", Scanned: 42, Indexed: 7, Skipped: 3, Hashed: 7, Bytes: 1023, Elapsed: 2 * time.Second})
	buf.Reset()

	p.finish("Interrupted")

	got := buf.String()
	if !strings.HasPrefix(got, "\r\x1b[K") || !strings.HasSuffix(got, "\n") {
		t.Errorf("summary should replace the status line and end it, got %q", got)
	}
	for _, want := range []string{"Interrupted", "/music", "42 entries scanned", "7 rows written", "3 skipped", "1023 B", "in 2s"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q missing %q", got, want)
		}
	}
}

func TestNilProgressIsSilent(t *testing.T) {
	var p *progress
	p.update(index.Progress{Root: "/data"})
	p.finish("Indexed")
}

func TestEllipsizeKeepsTail(t *testing.T) {
	if got := ellipsize("/short/path", 11); got != "/short/path" {
		t.Errorf("short path = %q, want unchanged", got)
	}
	long := strings.Repeat("a", 20) + "/" + strings.Repeat("b", 10)
	got := ellipsize(long, 13)
	if got != "..."+strings.Repeat("b", 10) {
		t.Errorf("ellipsize = %q", got)
	}
	if len([]rune(got)) > 13 {
		t.Errorf("ellipsize grew past the limit: %q", got)
	}
}

func TestGroupCount(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{{0, "0"}, {999, "999"}, {1000, "1,000"}, {12304, "12,304"}, {1234567890, "1,234,567,890"}} {
		if got := groupCount(tc.n); got != tc.want {
			t.Errorf("groupCount(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{{0, "0 B"}, {1023, "1023 B"}, {1024, "1.0 KiB"}, {3<<30 + 1<<29, "3.5 GiB"}, {5 << 40, "5.0 TiB"}} {
		if got := humanBytes(tc.n); got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestFlagWasSet(t *testing.T) {
	fs := flag.NewFlagSet("gocate", flag.ContinueOnError)
	enabled := fs.Bool("progress", true, "")
	if err := fs.Parse([]string{}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if flagWasSet(fs, "progress") {
		t.Error("defaulted flag reported as set")
	}
	if !*enabled {
		t.Fatal("default should be on")
	}

	if err := fs.Parse([]string{"-progress=false"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !flagWasSet(fs, "progress") {
		t.Error("explicitly set flag reported as unset")
	}
}
