package main

import (
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"strings"

	"encoding/hex"
	"path/filepath"

	"github.com/minio/highwayhash"
	"modernc.org/ql"
)

// TODO
// https://godoc.org/github.com/rjeczalik/notify
// debug output
// finish stats output

// cli flags
var updatedbFlag = flag.Bool("updatedb", false, "Update the database")
var gocateDir = flag.String("config", filepath.Join(os.Getenv("HOME"), ".gocate"), "Directory to store config and file DB")
var updatePath = flag.String("path", ".", "Path to walk and update")
var printDupes = flag.Bool("dupes", false, "Print duplicate files based on hash")
var stats = flag.Bool("stats", false, "Print DB stats")

// These all get used in the walkFunc, but we need to init them outside that function
var walkInsertQuery ql.List
var walkSelectQuery ql.List
var walkUpdateQuery ql.List
var walkHash hash.Hash
var walkDB *ql.DB
var walkCtx *ql.TCtx

func walkFunc(path string, info os.FileInfo, err error) error {
	if err != nil {
		log.Println("failed filepath.Walk function", err)
		return err
	}

	if info.IsDir() {
		return nil
	}

	file, ferr := os.Open(path)
	if ferr != nil {
		log.Println("Error opening: ", ferr, path)
	}
	defer file.Close()

	if _, copyErr := io.Copy(walkHash, file); copyErr != nil {
		log.Println("Error hashing", copyErr)
		return copyErr
	}
	hashInBytes := walkHash.Sum(nil)
	hashHex := hex.EncodeToString(hashInBytes)

	rs, _, selErr := walkDB.Execute(walkCtx, walkSelectQuery, path)
	if selErr != nil {
		log.Println("Select failed", selErr)
	}
	fr, err := rs[0].FirstRow()
	if err != nil {
		log.Println("Failed fetching firstrow", err)
	}
	if len(fr) == 0 {
		// no record, insert data into DB
		_, _, exErr := walkDB.Execute(walkCtx, walkInsertQuery, path, info.Size(), info.ModTime(), hashHex)
		if exErr != nil {
			log.Println("Failed to db.Execute", exErr)
		}
	}
	if len(fr) != 0 {
		if fr[3] != hashHex {
			// mismatched hashes, update data in DB
			_, _, exErr := walkDB.Execute(walkCtx, walkUpdateQuery, path, info.Size(), info.ModTime(), hashHex)
			if exErr != nil {
				log.Println("Failed to db.Execute", exErr)
			}
		}

	}

	return nil

}

// updatedb - Update the files DB by walking the path specified
// TODO:
// * Do hashing/db update in goroutines per CPU?
// * BEGIN/COMMIT outside of the loop?
func updatedb(ctx *ql.TCtx, path string, hh hash.Hash, db *ql.DB) {

	// precompile the INSERT command
	var compileErr error
	walkInsertQuery, compileErr = ql.Compile(`
		BEGIN TRANSACTION;
			INSERT INTO files VALUES($1, $2, $3, $4);
		COMMIT;
	`)
	if compileErr != nil {
		log.Println("Failed to compile insert", compileErr)
	}

	// precompile SELECT command
	walkSelectQuery, compileErr = ql.Compile(`
		SELECT * FROM files WHERE filename == $1
	`)
	if compileErr != nil {
		log.Println("Failed to compile select", compileErr)
	}

	// precompile the UPDATE command
	walkUpdateQuery, compileErr = ql.Compile(`
		BEGIN TRANSACTION;
			UPDATE files
				size = $2,
				modtimestamp = $3,
				hash = $4
			WHERE filename = $1;
		COMMIT;
	`)
	if compileErr != nil {
		log.Println("Failed to compile select", compileErr)
	}

	walkErr := filepath.Walk(path, walkFunc)
	if walkErr != nil && walkErr != filepath.SkipDir {
		log.Println("error walking", walkErr)
	}
}

// printDuplicates - Print out filenames line by line of duplicate files (based on hash value in DB)
// TODO:
// * Figure out how to get the list of duplicates directly from SQL if possible
func printDuplicates(ctx *ql.TCtx, db *ql.DB) {
	sel, compileErr := ql.Compile(`SELECT filename, hash from files;`)
	if compileErr != nil {
		log.Println("Failed to compile select", compileErr)
	}
	rs, _, selErr := db.Execute(ctx, sel)
	if selErr != nil {
		log.Println("Select failed", selErr)
	}
	hashMap := make(map[string][]string)
	for _, r := range rs {
		if err := r.Do(false, func(data []interface{}) (bool, error) {
			filename := data[0].(string)
			hhash := data[1].(string)
			if hashMap[hhash] == nil {
				hashMap[hhash] = make([]string, 0)
			}
			hashMap[hhash] = append(hashMap[hhash], filename)
			return true, nil
		}); err != nil {
			log.Fatalln(err)
		}
	}
	for _, filelist := range hashMap {
		if len(filelist) > 1 {
			fmt.Println(strings.Join(filelist, " "))
		}
	}
}

// printDB - print the entire ql DB contents
func printDB(ctx *ql.TCtx, db *ql.DB) {
	rss, _, selErr := db.Run(ctx, "SELECT * FROM files;")
	if selErr != nil {
		log.Println("Select failed", selErr)
	}

	log.Println("----")
	for _, rs := range rss {
		if err := rs.Do(false, func(data []interface{}) (bool, error) {
			log.Println(data)
			return true, nil
		}); err != nil {
			log.Fatalln(err)
		}
	}
	log.Println("----")
}

func searchDB(ctx *ql.TCtx, db *ql.DB, search string) {
	rss, _, selErr := db.Run(ctx, "SELECT * FROM files WHERE filename LIKE $1;", search)
	if selErr != nil {
		log.Println("Select failed", selErr)
	}

	for _, rs := range rss {
		if err := rs.Do(false, func(data []interface{}) (bool, error) {
			fmt.Println(data[0])
			return true, nil
		}); err != nil {
			log.Fatalln(err)
		}
	}
}

func printStats(ctx *ql.TCtx, db *ql.DB) {
	stats, err := db.Info()
	if err != nil {
		log.Println("failed to get db info")
	}
	log.Println(stats)
}

func main() {
	// log.Println("gocate... a fancy locate replacement written in Go")

	flag.Parse()

	// ensure needed paths exist
	if err := os.MkdirAll(*gocateDir, 0775); err != nil {
		log.Println("Failed to create dir:", &gocateDir, err)
	}

	// open database
	dbFile := filepath.Join(*gocateDir, "files.db")
	db, err := ql.OpenFile(dbFile, &ql.Options{CanCreate: true, FileFormat: 2})
	if err != nil {
		log.Fatalln(err)
	}

	// Setup tables for storing file data
	ctx := ql.NewRWCtx()
	_, _, err = db.Run(ctx, `
	BEGIN TRANSACTION;
		CREATE TABLE IF NOT EXISTS files (filename string, size int64, modtimestamp time, hash string);
	COMMIT;`)
	if err != nil {
		log.Fatalln(err)
	}

	searchPath, err := filepath.Abs(*updatePath)
	if err != nil {
		log.Fatalln("failed to get update path absolute path")
	}

	// Create highwayhash instance
	key, decodeErr := hex.DecodeString("9d6a5ccfe55ce0fa167002bd76b6409d66e4cfec57d1827e802ca2f5f6a3")
	if decodeErr != nil {
		log.Panicln("decode string failed", decodeErr)
	}
	hh, hashErr := highwayhash.New(key[:32])
	if hashErr != nil {
		log.Panicln("Failed to create HighwayHash instance:", hashErr) // add error handling
	}

	if *updatedbFlag {
		updatedb(ctx, searchPath, hh, db)
	}

	if *printDupes {
		printDuplicates(ctx, db)
	}

	if *stats {
		printStats(ctx, db)
	}

	if flag.NArg() > 0 {
		search := flag.Arg(0)
		searchDB(ctx, db, search)
	}

	if false {
		printDB(ctx, db)
	}
}
