package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-isatty"

	"github.com/iggy/gocate/internal/index"
)

// spinnerFrames animate the live status line so a run that is busy hashing one
// huge file still visibly moves between counter updates.
const spinnerFrames = `|/-\`

// maxRootRunes caps how much of the indexed path is echoed on the status line,
// so a deep root cannot wrap it onto a second line and break the redraw.
const maxRootRunes = 60

// progress renders an indexing run's status onto a single terminal line,
// rewriting it in place. A nil *progress is a valid silent sink, which is what
// an unattended run (cron, a pipe, -progress=false) hands to the indexer, so
// nothing is written anywhere.
type progress struct {
	w     io.Writer
	frame int
	last  index.Progress
}

// progressEnabled decides whether an indexing run should report progress: the
// -progress flag must be on, and unless it was set explicitly the run has to be
// attached to a terminal. Redirecting stderr to a file or to cron's mail
// therefore stays as quiet as it always was, while -progress=true forces the
// status line even when the caller knows better.
func progressEnabled() bool {
	if !*progressFlag {
		return false
	}
	return flagWasSet(flag.CommandLine, "progress") || isTerminal(os.Stderr)
}

// flagWasSet reports whether name appeared on the command line, as opposed to
// still holding its default.
func flagWasSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

func isTerminal(f *os.File) bool {
	fd := f.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

// update redraws the status line. It is the callback handed to the indexer, so
// it must tolerate a nil receiver.
func (p *progress) update(pr index.Progress) {
	if p == nil {
		return
	}
	p.last = pr
	frame := spinnerFrames[p.frame%len(spinnerFrames)]
	root := ellipsize(pr.Root, maxRootRunes)
	elapsed := pr.Elapsed.Round(time.Second)

	line := fmt.Sprintf("%c %s", frame, root)
	if pr.Counting {
		// The tree is still being counted, so there is nothing indexed to report
		// yet. The spinner keeps moving: a slow count of a huge tree should not
		// look like a hang.
		line += fmt.Sprintf("  counting %s entries so far  %s",
			groupCount(int64(pr.Scanned)), elapsed)
	} else {
		line += fmt.Sprintf("  %s scanned  %s indexed  %s hashed  %s",
			scanned(pr), groupCount(int64(pr.Indexed)), humanBytes(pr.Bytes), elapsed)
	}
	p.frame++
	p.draw("%s", line)
}

// draw replaces the single status line, which stays unterminated until finish
// ends it.
//
// Write errors are ignored on purpose: the line is cosmetic, and a lost stderr
// (a closed pipe, a terminal hangup) must not abort an indexing run that is
// otherwise healthy, nor bury the run's real output in write errors.
func (p *progress) draw(format string, args ...any) {
	_, _ = fmt.Fprintf(p.w, "\r\x1b[K"+format, args...)
}

// scanned renders the scan counter as "done/total" when the tree was counted up
// front, and as a bare count when no total is known. The counted total is only a
// snapshot: files created or removed mid-run, and entries whose stat fails, mean
// the two walks can disagree, so the denominator never falls below what has
// already been scanned rather than showing a run stuck past 100%.
func scanned(pr index.Progress) string {
	n := groupCount(int64(pr.Scanned))
	if pr.Total <= 0 {
		return n
	}
	return n + "/" + groupCount(int64(max(pr.Total, pr.Scanned)))
}

// finish ends the status line and writes a one-line summary of the run,
// prefixed by verb ("Indexed", "Interrupted").
func (p *progress) finish(verb string) {
	if p == nil {
		return
	}
	pr := p.last
	p.draw("%s %s: %s entries scanned, %s rows written, %s skipped, %s hashed in %s\n",
		verb,
		ellipsize(pr.Root, maxRootRunes),
		scanned(pr),
		groupCount(int64(pr.Indexed)),
		groupCount(int64(pr.Skipped)),
		humanBytes(pr.Bytes),
		pr.Elapsed.Round(time.Second),
	)
}

// ellipsize keeps the tail of a path that is longer than max runes, which is the
// part that identifies it, and marks the cut with an ellipsis.
func ellipsize(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return "..." + string(r[len(r)-(max-3):])
}

// groupCount renders n with thousands separators, e.g. 12304 -> "12,304".
func groupCount(n int64) string {
	digits := strconv.FormatInt(n, 10)
	var b strings.Builder
	b.Grow(len(digits) + (len(digits)-1)/3)
	for i, c := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// humanBytes renders a byte count with a binary unit suffix, e.g. 3758096384 ->
// "3.5 GiB".
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
