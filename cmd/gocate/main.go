package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"path/filepath"
	"runtime/pprof"

	"github.com/kalafut/imohash"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zeebo/xxh3"
	"modernc.org/ql"
)

// TODO
//   - https://godoc.org/github.com/rjeczalik/notify
//   - debug output
//   - finish stats output
//   - prune - remove old entries from DB
//   - something to skip hashing
//   - store multiple hashes i.e. imohash (can have collisions) and xxHash (no collisions)
//   - add a "deleted" flag to the DB
//   - don't hash 0 byte files

// cli flags
var updatedbFlag = flag.Bool("updatedb", false, "Update the database")
var gocateDir = flag.String("config", filepath.Join(os.Getenv("HOME"), ".gocate"), "Directory to store config and file DB")
var updatePath = flag.String("path", ".", "Path to walk and update")
var printDupes = flag.Bool("dupes", false, "Print duplicate files based on hash")
var stats = flag.Bool("stats", false, "Print DB stats")
var quick = flag.Bool("quick", false, "quick update (don't hash files that are in the database already)")
var noHash = flag.Bool("no-hash", false, "don't hash files, just add them to the database")
var hostname = flag.String("hostname", "", "custom hostname to use for the database")
var profile = flag.Bool("profile", false, "generate profile data")

// These all get used in the walkFunc, but we need to init them outside that function
var walkInsertQuery ql.List
var walkSelectQuery ql.List
var walkUpdateQuery ql.List

// var hHash hash.Hash
var fdb *ql.DB
var dbCtx *ql.TCtx
var worker chan fileInfo
var walkerDone chan bool
var wg sync.WaitGroup // Add WaitGroup to track goroutines

type fileInfo struct {
	Path     string
	Size     int64
	ModTime  time.Time
	Imohash  string // the main file hash... if there are collisions, we use xxh3
	XXH3Hash string // a fast hash that has no collisions
}

func hasher(worker chan<- fileInfo, file *fileInfo) {
	defer wg.Done()
	log.Trace().Interface("file", file).Msg("hasher started")

	f, err := os.Open(file.Path)
	if err != nil {
		log.Error().Err(err).Str("path", file.Path).Msg("failed to open dirent")
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error().Err(err).Str("path", file.Path).Msg("failed to close file")
		}
	}()
	log.Trace().Str("path", file.Path).Msg("opened dirent")

	// Calculate imohash
	data, err := os.ReadFile(file.Path)
	if err != nil {
		log.Error().Err(err).Str("path", file.Path).Msg("failed to read file")
		return
	}
	imo := imohash.Sum(data)
	file.Imohash = fmt.Sprintf("%x", imo)

	// Calculate xxh3 hash
	xxh3Hash := xxh3.Hash(data)
	file.XXH3Hash = fmt.Sprintf("%x", xxh3Hash)

	log.Trace().Interface("file", file).Msg("hasher result")
	worker <- *file
}

func walkFunc(path string, info os.FileInfo, err error) error {
	log.Trace().Str("path", path).Bool("isdir", info.IsDir()).Int64("size", info.Size()).Msg("walkFunc file")
	if err != nil {
		log.Error().Err(err).Msg("failed filepath.Walk function")
		return err
	}

	lt := info.Mode().Type()
	if lt == fs.ModeDir ||
		lt == fs.ModeSymlink ||
		lt == fs.ModeSocket ||
		lt == fs.ModeDevice ||
		lt == fs.ModeNamedPipe ||
		lt == fs.ModeCharDevice ||
		lt == fs.ModeIrregular ||
		*quick ||
		!*noHash {
		fi := fileInfo{Path: path, Size: info.Size(), ModTime: info.ModTime()}
		log.Trace().Interface("fileinfo", fi).Msg("sending non-regular file to worker channel")
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker <- fi
		}()
	} else {
		fi := fileInfo{
			Path:    path,
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}
		log.Trace().Interface("fileInfo", fi).Str("path", path).Msg("sending to worker")
		wg.Add(1)
		go hasher(worker, &fi)
	}

	return nil
}

// updatedb - Update the files DB by walking the path specified
// TODO:
//   - Do hashing/db update in goroutines per CPU?
//   - BEGIN/COMMIT outside of the loop?
//   - split entering hostname/size/timestamp from hashing
func updatedb(path string) {
	var hn string
	var err error
	if *hostname != "" {
		hn = *hostname
	} else {
		hn, err = os.Hostname()
		if err != nil {
			log.Error().Err(err).Msg("failed to get hostname")
			hn = "unknown"
		}
	}
	// precompile the INSERT command
	// TODO: batch insert?
	walkInsertQuery, err = ql.Compile(fmt.Sprintf(`
		BEGIN TRANSACTION;
			INSERT INTO files VALUES("%s", $1, $2, $3, $4, $5);
		COMMIT;
	`, hn))
	if err != nil {
		log.Error().Err(err).Msg("failed to compile insert")
	}

	// precompile SELECT command
	walkSelectQuery, err = ql.Compile(fmt.Sprintf(`
		SELECT * FROM files WHERE hostname == "%s" && filename == $1
	`, hn))
	if err != nil {
		log.Error().Err(err).Msg("failed to compile select")
	}

	// precompile the UPDATE command
	updateQuery := fmt.Sprintf(`
		BEGIN TRANSACTION;
			UPDATE files SET
				hostname = "%s",
				size = $2,
				modtimestamp = $3,
				imohash = $4,
				xxh3hash = $5
			WHERE filename = $1;
		COMMIT;
	`, hn)
	walkUpdateQuery, err = ql.Compile(updateQuery)
	if err != nil {
		log.Error().Err(err).Msg("failed to compile update")
	}

	// setup context
	dbCtx = ql.NewRWCtx()

	walkErr := filepath.Walk(path, walkFunc)
	log.Trace().Msg("updatedb walker done")
	if walkErr != nil && walkErr != filepath.SkipDir {
		log.Error().Err(walkErr).Msg("failed to walk path")
	}

	// Wait for all goroutines to finish
	wg.Wait()
	walkerDone <- true
	// Don't close the worker channel here - let the main loop handle it
}

// printDuplicates - Print out filenames line by line of duplicate files (based on hash value in DB)
// TODO:
// * Figure out how to get the list of duplicates directly from SQL if possible
func printDuplicates() {
	sel, err := ql.Compile(`SELECT filename, imohash, xxh3hash from files;`)
	if err != nil {
		log.Error().Err(err).Msg("failed to compile select")
	}
	rs, _, selErr := fdb.Execute(dbCtx, sel)
	if selErr != nil {
		log.Error().Err(selErr).Msg("Select failed")
	}
	hashMap := make(map[string][]string)
	for _, r := range rs {
		if err := r.Do(false, func(data []interface{}) (bool, error) {
			filename := data[0].(string)
			// Use xxh3hash as the key since it has no collisions
			hhash := data[2].(string)
			if hashMap[hhash] == nil {
				hashMap[hhash] = make([]string, 0)
			}
			hashMap[hhash] = append(hashMap[hhash], filename)
			return true, nil
		}); err != nil {
			log.Fatal().Err(err).Msg("Failed fetching rows")
		}
	}
	for _, filelist := range hashMap {
		if len(filelist) > 1 {
			fmt.Println(strings.Join(filelist, " "))
		}
	}
}

// printDB - print the entire ql DB contents
func printDB() {
	rss, _, selErr := fdb.Run(dbCtx, "SELECT * FROM files;")
	if selErr != nil {
		log.Error().Err(selErr).Msg("Select failed")
	}

	fmt.Println("----")
	for _, rs := range rss {
		if err := rs.Do(false, func(data []interface{}) (bool, error) {
			fmt.Println(data)
			return true, nil
		}); err != nil {
			log.Fatal().Err(err).Msg("printDB: Failed fetching rows")
		}
	}
	fmt.Println("----")
}

// TODO this should only open the DB in read-only mode
func searchDB(search string) {
	searchResult, _, err := fdb.Run(dbCtx, "SELECT * FROM files WHERE filename LIKE $1;", search)
	if err != nil {
		log.Error().Err(err).Msg("Select failed")
	}

	for _, rs := range searchResult {
		if err := rs.Do(false, func(data []interface{}) (bool, error) {
			fmt.Println(data)
			return true, nil
		}); err != nil {
			log.Fatal().Err(err).Msg("searchDB: Failed fetching rows")
		}
	}
}

func printStats() {
	stats, err := fdb.Info()
	if err != nil {
		log.Error().Err(err).Msg("failed to get db info")
	}
	fmt.Println(stats.Name, stats.Tables)
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	// log.Println("gocate... a fancy locate replacement written in Go")

	var err error

	flag.Parse()

	if *profile {
		f, err := os.Create("default.pgo")
		if err != nil {
			log.Fatal().Err(err).Msg("could not create CPU profile: ")
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Error().Err(err).Msg("failed to close file")
			}
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Error().Err(err).Msg("could not start CPU profile: ")
		}
		defer pprof.StopCPUProfile()
	}

	// ensure needed paths exist
	// TODO should we create this or just bail if it doesn't exist?
	if err = os.MkdirAll(*gocateDir, 0775); err != nil {
		log.Error().Err(err).Str("dir", *gocateDir).Msg("Failed to create dir")
	}

	// open database
	dbFile := filepath.Join(*gocateDir, "files.db")
	fdb, err = ql.OpenFile(dbFile, &ql.Options{CanCreate: true, FileFormat: 2})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open DB")
	}

	// Setup tables for storing file data
	dbCtx := ql.NewRWCtx()
	_, _, err = fdb.Run(dbCtx, `
	BEGIN TRANSACTION;
		CREATE TABLE IF NOT EXISTS files (
			hostname string,
			filename string,
			size int64,
			modtimestamp time,
			imohash string,
			xxh3hash string,
		);
	COMMIT;`)
	// ALTER TABLE files ADD xxh3hash string;
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create table")
	}

	searchPath, err := filepath.Abs(*updatePath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get update path absolute path")
	}

	// Create highwayhash instance
	// key, err := hex.DecodeString("9d6a5ccfe55ce0fa167002bd76b6409d66e4cfec57d1827e802ca2f5f6a3")
	// if err != nil {
	// 	log.Panic().Err(err).Msg("Failed to decode key")
	// }
	// hHash, err = highwayhash.New(key[:32])
	// if err != nil {
	// 	log.Panic().Err(err).Msg("Failed to create HighwayHash instance")
	// }

	if *updatedbFlag {
		worker = make(chan fileInfo)
		walkerDone = make(chan bool, 1)
		log.Trace().Msg("starting worker")

		go updatedb(searchPath)

		log.Trace().Msg("waiting for walker to be done")

	while:
		for {
			select {
			case fi := <-worker:
				log.Trace().Interface("file", fi).Msg("hasher finished")
				rs, _, selErr := fdb.Execute(dbCtx, walkSelectQuery, fi.Path)
				if selErr != nil {
					log.Error().Err(selErr).Msg("Select failed")
				}
				fr, err := rs[0].FirstRow()
				if err != nil {
					log.Error().Err(err).Msg("Failed fetching firstrow")
				}
				if len(fr) == 0 {
					_, _, exErr := fdb.Execute(dbCtx, walkInsertQuery, fi.Path, fi.Size, fi.ModTime, fi.Imohash, fi.XXH3Hash)
					if exErr != nil {
						log.Error().Err(exErr).Msg("Insert failed to db.Execute")
					}
				}
				if len(fr) != 0 && !*quick {
					if fr[3] != fi.Imohash || fr[4] != fi.XXH3Hash {
						_, _, exErr := fdb.Execute(dbCtx, walkUpdateQuery, fi.Path, fi.Size, fi.ModTime, fi.Imohash, fi.XXH3Hash)
						if exErr != nil {
							log.Error().Err(exErr).Msg("Update failed to db.Execute")
						}
					}
				}
			case <-walkerDone:
				log.Trace().Msg("walker done")
				// Wait a bit for any remaining goroutines to finish
				time.Sleep(2 * time.Second)
				close(worker)
				break while
			}
		}
	}

	if *printDupes {
		printDuplicates()
	}

	if *stats {
		printStats()
		printDB()
	}

	if flag.NArg() > 0 {
		search := flag.Arg(0)
		searchDB(search)
	}

	if false {
		printDB()
	}
}
