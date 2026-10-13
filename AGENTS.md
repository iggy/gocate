# AGENTS.md

Guide for AI agents working in the `gocate` repository — a `locate`-like CLI written in Go that indexes filesystem metadata and content hashes into an embedded SQL database.

## Essential commands

```sh
go build ./cmd/gocate                   # plain build -> ./gocate
task build                              # static CGO_ENABLED=0 build with stripped symbols
go test ./...                           # run all tests (also what CI runs)
go test ./internal/store/...            # test one package
go fmt ./...                            # preferred formatting (not bare gofmt)
golangci-lint run                       # linter used by CI (golangci-lint-action@v9.2.1)
```

There is no Makefile. Builds and tests go through `go` directly or `task` (see `Taskfile.yml`). CGO is disabled in the `task build` target (`CGO_ENABLED=0`) with static ldflags — preserve this when editing the Taskfile.

## CI

Three workflows run on every push (`test.yml`, `megalinter.yml`) and on master/PRs (`codeql-analysis.yml`):

- **Test**: `go test ./...` on Ubuntu, Go version pinned from `go.mod` (currently `go 1.25.0`), plus `golangci-lint`. A green test run requires both to pass.
- **Megalinter** (Go flavor): `DISABLE_LINTERS: SPELL_CSPELL`, `GO_REVIVE_CLI_LINT_MODE: list_of_files`. `jscpd` (copy-paste detection) is configured via `.jscpd.json` to **ignore `*_test.go`** — do not be alarmed by similar test helpers across packages. Repo-level linter configs: `revive.toml`, `.github/zizmor.yml`, `osv-scanner.toml`. MegaLinter 10 removed `project` from revive's supported CLI lint modes, which is why `revive.toml` drops the `package-comments` rule — with a file list (not a package) revive reports every file in a package that is not the one holding the package doc, so the rule is unactionable there. `zizmor` is new in v10 and its unpinned-uses default was tightened to require SHA pins; `.github/zizmor.yml` relaxes that to `ref-pin` because Dependabot keeps the action tags current.
- **release.yml** uses deprecated `actions/create-release` / `upload-release-asset` (marked TODO in the file) and pins Go 1.18. Touch only if touching releases.

## Architecture

Three packages, strict layering:

```
cmd/gocate        CLI: flag parsing, output formatting, profile lifecycle
internal/index    Filesystem walk + bounded concurrent hashing pipeline
internal/store    Embedded modernc.org/ql database: schema, upsert, search, dupes
```

`cmd/gocate` is the only `package main`. `internal/...` is not importable outside the module.

### Control flow

`main.run()` parses flags, opens a `store.Store`, then dispatches in a fixed order (not mutually exclusive): `-updatedb` → `-dupes` → `-dupes-script` → `-stats` → `-checksum` → positional search pattern. All actions run against the same store handle; order matters because `-updatedb` must complete before queries see new rows.

`index.VerifyCtx` (`internal/index/verify.go`) backs `-checksum`: same walk plus bounded hasher pool as `RunCtx`, but it never writes to the store. It classifies each regular file with the cheapest conclusive check (`compareContents`): no row in the snapshot means `unindexed` without reading the file at all, an imohash mismatch means `modified` without reading the file beyond imohash's own cost, and only files whose samples still match pay a full pass to confirm equality. `modified` also covers rows holding no hashes (`-no-hash`) and unreadable files, since neither can be confirmed. Workers append drift under a mutex, the report is sorted by path so repeat runs are byte-identical, and `-checksum` exits 1 when anything is reported.

### Indexing pipeline (`internal/index`)

`index.RunCtx` (and the thin `Run` wrapper) is a bounded producer/consumer fan-out, **not** a worker pool over a channel of paths. `Options.Workers` (CLI `-workers`, 0 = one per CPU) is the cap on concurrent hashes:

1. **Snapshot load**: one full-table `LoadExisting()` builds a `map[string]FileInfo` of rows already on disk for this host. The map is **read-only after construction** and shared between the walk goroutine (quick-mode skip checks) and the consumer (insert-vs-update decisions) **without locking** — no per-row `Has()`/`firstRow()` SELECT is performed during indexing, which used to be an O(n²) full-table-scan sequence and the dominant CPU cost.
2. `filepath.Walk` runs on the calling goroutine. `classify` decides per entry: `actionSkip` (quick mode and the file is already in the snapshot — the walk emits nothing, so stored rows and hashes are never rewritten), `actionRecord` (dirs, symlinks, `-no-hash`), or `actionHash`. For a file to hash, it acquires a slot from a `semCount` channel (cap = `workers`, default `runtime.NumCPU()`) **and** reserves `residentBytes(info.Size())` against a `byteBudget` (`internal/index/budget.go`, default `defaultMaxBytes(workers)` — one worker's worth of content per worker) guarded by a condvar. The charge is what a hash actually holds in memory (the whole file below `imohash.SampleThreshold`, `streamReadBuffer` above it), **never the file's size**, because hashing streams. Charging sizes was the throughput bug: a directory of multi-gigabyte files fought over the budget and hashed one at a time on a single core. `actionRecord` entries go straight to the results channel from the walk goroutine — no semaphore slot, no byte budget. The consumer never replaces stored hashes with empty ones when size and modtime are unchanged, so a `-no-hash` pass cannot erase the duplicate index.
3. Worker goroutines call `hashFile` and send `store.FileInfo` on the unbuffered `results` channel.
4. A single consumer goroutine drains `results` and accumulates rows into a `WriteBatch` (one transaction) committed when `len(batch) >= BatchRows` (default 1000) **or** `sum(fi.Size) >= BatchBytes` (default 4 GiB). The byte cap bounds how much hashing effort a Ctrl-C can forfeit: at most one in-flight batch. Deciding insert-vs-update from the in-memory snapshot (rather than per-row SELECTs) makes the consumer near-instant, which is what lets the parallel hashers stay busy.
5. After `Walk` returns: `wg.Wait()`, `close(results)`, the consumer flushes its final partial batch, then `<-consumerDone` returns.

`RunCtx` takes a `context.Context`; `main.run()` cancels it on SIGINT/SIGTERM so the walk stops and the consumer flushes its in-flight batch (committing partial progress) before `run()` returns and `s.Close()` rolls back/open-clean the store. A **second** SIGINT calls `os.Exit(130)` immediately, for the case where the flush itself is stuck. A cancelled run returns `context.Canceled` (walk errors are wrapped, cancellation is passed through bare), so callers can tell an interrupted run from a complete one; `main` uses that to say "Interrupted" instead of "Indexed".

**Progress reporting** (`internal/index/progress.go`): `Options.Progress func(Progress)` is off by default. When set, a `tracker` of atomic counters (walk increments `scanned`/`skipped`, hashers `hashed`/`bytes`, the consumer `indexed`) is polled by one reporter goroutine every `ProgressInterval` (default 500ms), and `stopReporting` — deferred inside `RunCtx` — delivers one **final** snapshot after the consumer has flushed, so a summary printed right after `RunCtx` returns never undercounts. Callbacks are serialized on that single goroutine and never overlap, so implementations need no locking. Counting stays enabled unconditionally because an atomic add is cheaper than branching at every call site. `cmd/gocate/progress.go` renders it as one line rewritten in place (`\r\x1b[K`) on stderr with an ASCII spinner, plus a closing summary; a nil `*progress` is a valid silent sink. Gating is `progressEnabled()`: `-progress=false` always wins, `-progress=true` forces output even into a pipe, and otherwise it requires a terminal (`go-isatty`, already an indirect dep via zerolog), which keeps cron and redirected runs as quiet as they always were.

**Totals**: `Options.CountTotal` (`-total`, on whenever progress is) does one extra `filepath.WalkDir` pass before indexing to count entries, giving the `24,004/38,012 scanned` denominator. `WalkDir` is used because the entry type comes from the directory listing — no per-file `Stat` — so the pass is negligible next to hashing, though it does double a `-no-hash` run over a slow tree, which is why it is opt-out. The count is a snapshot, not a promise: entries created mid-run, removed entries, and entries whose `lstat` fails (which `Walk` reports but `WalkDir` never sees) all make the two walks disagree, so the renderer caps the denominator at the scanned count instead of showing a run past 100%. While the pre-pass runs, snapshots carry `Counting: true` and report `Scanned` as the counted-so-far number, so the spinner keeps moving on a tree too big to list quickly.

Backpressure comes from the two semaphores, not the channel — `results` is unbuffered, so a fast walk blocks on slow consumers. Do not add buffering without thinking about memory bounds on huge trees: `workers` caps open files and goroutines, `byteBudget` caps the content they hold. That budget must stay charged in resident bytes — recharging whole file sizes turns a directory of large files back into a one-core run.

### Hashing (`internal/index/hash.go`)

Two content hashes per file, both streaming (no whole-file read into memory):

- **imohash** (`github.com/kalafut/imohash`): murmur3-128 over the file size plus a fixed number of sample bytes (whole file below `imohash.SampleThreshold` = 128 KiB; above it, 16 KiB each from the head, `size/2` and the tail). Fast, and its inference is one-directional: **differing imohash proves the contents differ**, matching imohash only makes equality likely because un-sampled bytes are invisible. Primary hash for cheap change detection.
- **xxh3** (`github.com/zeebo/xxh3`): full-content streaming hash treated as collision-free. Used for duplicate detection, so it must be current for every row a run writes.

`contentHasher` (hash.go) opens a file once, `Stat`s it, and reads only what the caller asks for: files up to `hashBufferLimit` (= `imohash.SampleThreshold`) are read once into a buffer that both hashes come from, larger files are never buffered (`sample()` runs imohash over a section reader, `full()` rewinds and streams xxh3). `hashFile` is the both-hashes convenience wrapper used by the indexer; `compareContents` in verify.go uses the tiers. **Zero-byte files return empty hashes with a nil error** — empty hashes are the sentinel for "unhashed", shared with `-no-hash` mode. `Duplicates()` explicitly filters `hash == ""`.

A pinning test (`TestHashFileMatchesReadFileBasis`) asserts the implementation is byte-identical to the old `os.ReadFile`-based one on both sides of the sample threshold, including its exact boundary. **Existing database rows depend on this**: do not change the hash output format without a migration story.

### Store (`internal/store`)

`modernc.org/ql` — a pure-Go (no CGO) embedded SQL database. Single table `files(hostname, filename, size, modtimestamp, imohash, xxh3hash)`. DB lives at `<configDir>/files.db`; default configDir is `~/.gocate`.

Concurrency model: a `sync.Mutex` on `Store` serializes **all** access. The indexer no longer relies on concurrent racy access, though: it snapshots the host's rows up front (`LoadExisting`) and decides insert-vs-update in-process, so `classify`/consumer no longer call `Has()`/`firstRow()` during a run. `Has`/`Upsert` remain for ad-hoc callers and tests.

Query compilation: `Open` precompiles `insertStmt`, `selectQ`, `updateStmt` via `ql.Compile` with the hostname **embedded as a string literal** (not a parameter). This is intentional — hostname comes from `os.Hostname()` or the CLI flag, never from search input, so it is not an injection vector. The `filename` column is always parameterized (`$1`). Note `insertStmt`/`updateStmt` deliberately carry **no `BEGIN TRANSACTION`/`COMMIT`** of their own so `execBatch`/`WriteBatch` can bracket many statements in one transaction (~200x faster than per-row commits). `Upsert` wraps its one-statement work in `execBatch` to stay transaction-safe.

Gotchas specific to ql:

- **`LIKE` is regex, not SQL globbing.** `Search(pattern)` passes the user's regex straight to `WHERE filename LIKE $1`. Indexed rows are searched by Go-flavored regex — see `store.go:198` and the test using `\.md$`.
- Uses `ql.NewRWCtx()` for both reads and writes — there is no read-only context. `ql.OpenFile` also takes an exclusive `fcntl` lock on a hidden lock file beside `files.db`, so a second `gocate` process cannot open the DB at all while an `-updatedb` run holds it; the README roadmap tracks serving read-only actions (search, `-dupes`, `-stats`, `-checksum`) off a snapshot or upstream read-only support.
- `Upsert`/`WriteBatch` decide insert-vs-update from the caller's knowledge (`isUpdate[i]`), not from a per-row SELECT. In **quick** mode the indexer skips existing rows entirely (by snapshot lookup); in **non-quick** mode an UPDATE is issued only when a hash changed, otherwise the row is dropped from the batch (a no-op). Existing rows left untouched even if stale is **by design** for incremental re-index.
- **`ql.Close` fails if a transaction is still open** ("cannot close DB while open transaction exist") and leaves a `.<sha>` WAL/lock file behind. The SIGINT path therefore does `ROLLBACK` (or lets `WriteBatch`'s final COMMIT complete) before `Close`; do not call `Close` mid-batch.

## Conventions

- **Module path**: `github.com/iggy/gocate`.
- **Errors**: wrap with `%w` and a short context prefix matching the operation (`"insert %q: %w"`, `"walk %q: %w"`). Don't lose the underlying error.
- **Logging**: `github.com/rs/zerolog`, global level set to `WarnLevel` in `run()`. Log to stderr via `zerolog.ConsoleWriter`. Use `log.Error().Err(err).Str("path", ...).Msg(...)` — structured fields, not interpolated strings.
- **Flags**: package-level `flag.Bool`/`flag.String` vars in `main.go` with descriptive names (`updatedbFlag`, `printDupes`, etc.). Access via pointer deref, not assignment.
- **Indentation**: tabs (Go standard `go fmt`).
- **No comments beyond doc comments and deliberate `// why` notes** (e.g. the `hash.go` comment explaining it must stay byte-identical to the old implementation). The codebase is otherwise sparse on inline comments.
- **Doc comments** on every exported identifier — enforced by `revive`'s `exported` rule (see `revive.toml`, which mirrors revive's default set minus `package-comments`).

## Testing

- `go test ./...` is the canonical command (CI uses it verbatim).
- Helpers: `store_test.go` has `openTest(t)`; `index` tests share `writeFile` / `buildTree` (in `hash_test.go` and `index_test.go` respectively — `buildTree` creates `f1.txt`/`f2.txt` identical, `sub/f3.txt` unique, plus a symlink).
- Tests use `t.TempDir()` for both DBs and fixture trees — no shared state, parallel-safe.
- `t.Helper()` is used in all test helpers; follow that pattern.
- Hash correctness is regression-tested against `imohash.Sum` / `xxh3.Hash` of the full bytes — if you change `hashFile`, update or justify breaking this test.

## Workflow

- **Commit finished work.** Once a feature or fix is complete — `go test ./...` green, `go fmt ./...` applied, `golangci-lint run` clean — commit it on the current branch without waiting to be asked. One commit per completed unit of work, not one per edited file.
- Message style: imperative, why-focused, first line under 72 characters (see the existing history, e.g. `perf: bound indexing memory and flush partial progress on interrupt`).
- Do **not** push, and never push to `origin/main`; committing locally is the stopping point.
- Leave uncommitted only work that is genuinely mid-flight or that the user asked to hold.

## Gotchas

- **`-quick` skips existing rows entirely, it does not detect deletions.** There is no "deleted" flag yet (README roadmap). Re-indexing a pruned tree leaves stale rows. Because skipped rows are never rewritten, quick mode also does not refresh their size/modtime.
- **Symlinks** are walked by `filepath.Walk` but `shouldHash` checks `info.Mode().IsRegular()`, so symlinks are recorded with path/size/modtime but **not hashed** even in normal mode.
- **`-dupes-script`** emits a `/bin/sh` hardlink script; `shellQuote` does POSIX single-quote escaping (`'` → `'"'"'`). Paths with single quotes are handled — don't reimplement quoting naively.
- **`-profile` writes `default.pgo`** in the CWD for PGO builds (`task build-profile`). The `Taskfile.yml` `build` and `build-profile` targets are currently identical; PGO requires running a profiled workload first to produce `default.pgo`.
- **Hostname is recorded per-row**, so the same DB can hold entries from multiple hosts (via `-hostname`). `Has`/`Upsert`/`Search`/`LoadExisting` all scope by the current run's hostname. `Dump` and `Duplicates` do **not** filter by hostname — they read all rows.
- **`release.yml` pins Go 1.18** while `go.mod` requires 1.25.0 — the release workflow is stale and will fail on a fresh tag. Do not "fix" it as part of an unrelated change; flag it to the maintainer.
