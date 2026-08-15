# gocate — a cleverer `locate` written in Go

`gocate` walks a filesystem tree, stores file metadata and content hashes in an
embedded SQL database (`~/.gocate/files.db`), and lets you search files by name
or list duplicate files by content hash.

## Features

- Indexes files by `(hostname, filename)` with size, mod time, and two content
  hashes (`imohash` for speed, `xxh3` as a collision-free tiebreaker).
- Regex filename search.
- Duplicate detection by content hash.
- Incremental (`-quick`) and metadata-only (`-no-hash`) indexing modes.

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
```

### Flags

| Flag         | Description                                              |
|--------------|----------------------------------------------------------|
| `-updatedb`  | Update the database by walking `-path`.                  |
| `-path`      | Path to walk and index (default `.`).                    |
| `-config`    | Directory holding the file DB (default `~/.gocate`).     |
| `-quick`     | Incremental update: skip files already in the database.  |
| `-no-hash`   | Record path/size/modtime only; don't hash file contents. |
| `-dupes`     | Print groups of duplicate files.                         |
| `-stats`     | Print DB stats and dump all rows.                        |
| `-hostname`  | Override the hostname recorded with each row.            |
| `-profile`   | Write a CPU profile to `default.pgo` (for PGO builds).   |

## Layout

```
cmd/gocate      # CLI: flag parsing and output
internal/store  # embedded SQL database: schema, upsert, search, duplicates
internal/index  # filesystem walk + bounded concurrent hashing pipeline
```

## Roadmap

- Prune entries for files that no longer exist (a "deleted" flag).
- Batch inserts for faster indexing.
- Live updates via filesystem notifications
  ([rjeczalik/notify](https://godoc.org/github.com/rjeczalik/notify)).
- Open the DB read-only for pure searches.
