# gocate — a cleverer `locate` written in Go

`gocate` walks a filesystem tree, stores file metadata and content hashes in an
embedded SQL database (`~/.gocate/files.db`), and lets you search files by name
or list duplicate files by content hash.

## Features

- Indexes files by `(hostname, filename)` with size, mod time, and two content
  hashes (`imohash` for speed, `xxh3` as a collision-free tiebreaker).
- Regex filename search.
- Duplicate detection by content hash.
- Integrity checking (`-checksum`): report files whose content no longer
  matches the stored hashes, or that were never indexed.
- Incremental (`-quick`) and metadata-only (`-no-hash`) indexing modes.
- Live one-line progress while indexing (`24,004/38,012 scanned`), automatically
  silent when stderr is not a terminal, so cron runs stay quiet.

## Install / build

Uses [Task](https://taskfile.dev) for a static `CGO_ENABLED=0` binary:

```sh
task build            # -> ./gocate
go build ./cmd/gocate # plain build without Task
```

## Usage

```sh
# Index a tree (hashing every regular file).
gocate -updatedb -path /
gocate -updatedb -path ~/Music

# Incremental re-index: skip files already in the database.
gocate -updatedb -path ~/Music -quick

# Index without hashing (path/size/modtime only).
gocate -updatedb -path / -no-hash

# Search filenames (the pattern is a regular expression).
gocate '\.md$'

# List groups of duplicate files (by content hash).
gocate -dupes

# Print DB info and dump all rows.
gocate -stats

# Verify file checksums against the database.
gocate -checksum -path /important/data

# Force the progress line even though stderr is not a terminal.
gocate -updatedb -path /var/log -progress=true 2>/tmp/index.log
```

### Flags

| Flag         | Description                                                    |
|--------------|----------------------------------------------------------|
| `-updatedb`  | Update the database by walking `-path`.                        |
| `-path`      | Path to walk and index (default `.`).                          |
| `-config`    | Directory holding the file DB (default `~/.gocate`).           |
| `-quick`     | Incremental update: skip files already in the database.        |
| `-no-hash`   | Record path/size/modtime only; don't hash file contents.       |
| `-workers`   | Files to hash at once (default: one per CPU).                  |
| `-progress`  | Show indexing progress on a terminal; quiet when piped.        |
| `-total`     | Count the tree first so progress shows a total.                |
| `-dupes`     | Print groups of duplicate files.                               |
| `-stats`     | Print DB stats and dump all rows.                              |
| `-hostname`  | Override the hostname recorded with each row.                  |
| `-profile`   | Write a CPU profile to `default.pgo` (for PGO builds).         |
| `-checksum`  | Verify checksums against the DB (exit 1 on drift).             |

## Layout

```text
cmd/gocate      # CLI: flag parsing and output
internal/store  # embedded SQL database: schema, upsert, search, duplicates
internal/index  # filesystem walk + bounded concurrent hashing pipeline
```

## Roadmap

- Prune entries for files that no longer exist (a "deleted" flag).
- Batch inserts for faster indexing.
- Optional `-trust-mtime`: skip hashing files whose size and modtime still match
  their row, so re-indexing an unchanged tree costs a walk and nothing else. It
  is weaker than hashing, since it misses edits that preserve mtime (`rsync -t`,
  tar extraction, `touch -d`), so it stays opt-in.
- Live updates via filesystem notifications
  ([rjeczalik/notify](https://godoc.org/github.com/rjeczalik/notify)).
- Open the DB read-only for actions that never write (plain searches, `-dupes`,
  `-dupes-script`, `-stats`, `-checksum`) so they can run while another process
  is indexing. Today they cannot: `ql.OpenFile` takes an exclusive `fcntl` lock
  on a hidden lock file beside `files.db` and offers no read-only option, so a
  second run fails to open the database until the indexing run exits. Either add
  read-only support upstream, or serve these actions from a snapshot copy of
  `files.db` taken while the writer is not holding it.
- ability to merge database files (i.e. have a local db and one that lives on a
  network fs that local hosts can push their database into)
