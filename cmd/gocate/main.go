package main

import (
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"strings"
	// "crypto/md5"
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

// updatedb - Update the files DB by walking the path specified
// TODO:
// 		 Do hashing/db update in goroutines per CPU?
//   	 BEGIN/COMMIT outside of the loop?
func updatedb(ctx *ql.TCtx, path string, hh hash.Hash, db *ql.DB) {

	// precompile the INSERT command
	ins, compileErr := ql.Compile(`
		BEGIN TRANSACTION;
			INSERT INTO files VALUES($1, $2, $3, $4);
		COMMIT;
	`)
	if compileErr != nil {
		log.Println("Failed to compile insert", compileErr)
	}

	// precompile SELECT command
	sel, compileErr := ql.Compile(`
		SELECT * FROM files WHERE filename == $1
	`)
	if compileErr != nil {
		log.Println("Failed to compile select", compileErr)
	}

	// precompile the UPDATE command
	upd, compileErr := ql.Compile(`
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

	walkErr := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		// log.Println(path)
		if err != nil {
			log.Println("failed filepath.Walk function", err)
			return err
		}

		if info.IsDir() {
			// log.Println("skipping", info.Name(), path)
			return nil
		}

		file, ferr := os.Open(path)
		if ferr != nil {
			log.Println("Error opening: ", ferr, path)
		}
		defer file.Close()

		if _, copyErr := io.Copy(hh, file); copyErr != nil {
			log.Println("Error hashing", copyErr)
			return copyErr
		}
		hashInBytes := hh.Sum(nil)
		hashHex := hex.EncodeToString(hashInBytes)

		rs, _, selErr := db.Execute(ctx, sel, path)
		if selErr != nil {
			log.Println("Select failed", selErr)
		}
		fr, err := rs[0].FirstRow()
		if err != nil {
			log.Println("Failed fetching firstrow", err)
		}
		// log.Println("resultset", rs[0], fr)
		if len(fr) == 0 {
			// no record, insert data into DB
			// log.Println("Inserting")
			_, _, exErr := db.Execute(ctx, ins, path, info.Size(), info.ModTime(), hashHex)
			if exErr != nil {
				log.Println("Failed to db.Execute", exErr)
			}
		}
		if len(fr) != 0 {
			if fr[3] != hashHex {
				// log.Println("match", fr[3], hashHex)

				// mismatched hashes, update data in DB
				// log.Println("Updating", path, fr[3], hashHex)
				_, _, exErr := db.Execute(ctx, upd, path, info.Size(), info.ModTime(), hashHex)
				if exErr != nil {
					log.Println("Failed to db.Execute", exErr)
				}
			}

		}

		return nil
	})
	if walkErr != nil && walkErr != filepath.SkipDir {
		log.Println("error walking", walkErr)
	}
}

// printDuplicates - Print out filenames line by line of duplicate files (based on hash value in DB)
// TODO:
// 		 Figure out how to get the list of duplicates directly from SQL if possible
func printDuplicates(ctx *ql.TCtx, db *ql.DB) {

	sel, compileErr := ql.Compile(`SELECT filename, hash from files;`)
	if compileErr != nil {
		log.Println("Failed to compile select", compileErr)
	}
	rs, _, selErr := db.Execute(ctx, sel)
	if selErr != nil {
		log.Println("Select failed", selErr)
	}
	// log.Println("resultset", rs)
	hashMap := make(map[string][]string)
	for _, r := range rs {

		// log.Println(r)
		if err := r.Do(false, func(data []interface{}) (bool, error) {
			filename := data[0].(string)
			hhash := data[1].(string)
			// log.Println(data, filename, hhash)
			if hashMap[hhash] == nil {
				hashMap[hhash] = make([]string, 0)
			}
			hashMap[hhash] = append(hashMap[hhash], filename)
			return true, nil
		}); err != nil {
			log.Fatalln(err)
		}
	}
	// for hhash, filelist := range hashMap {
	for _, filelist := range hashMap {
		// log.Printf("filelist: %+v (%d)", filelist, len(filelist))
		if len(filelist) > 1 {
			// log.Println("hhash:filelist", hhash, filelist)
			fmt.Println(strings.Join(filelist, " "))
		}
	}
	// log.Println(hashMap)

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
	// log.Println("search pattern: ", search)

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
	stats, _ := db.Info()
	log.Println(stats)
}

func main() {
	// log.Println("gocate... a fancy locate replacement written in Go")

	flag.Parse()

	// ensure needed paths exist
	// log.Printf("Creating gocate dir if necessary (%v)\n", *gocateDir)
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

	searchPath, _ := filepath.Abs(*updatePath)

	// Create highwayhash instance
	key, decodeErr := hex.DecodeString("9d6a5ccfe55ce0fa167002bd76b6409d66e4cfec57d1827e802ca2f5f6a3")
	if decodeErr != nil {
		log.Panicln("decode string failed", decodeErr)
	}
	hh, hashErr := highwayhash.New(key[:32])
	if hashErr != nil {
		log.Panicln("Failed to create HighwayHash instance:", hashErr) // add error handling
	}

	if *updatedbFlag == true {
		updatedb(ctx, searchPath, hh, db)
	}

	if *printDupes == true {
		printDuplicates(ctx, db)
	}

	if *stats == true {
		printStats(ctx, db)
	}

	if flag.NArg() > 0 {
		search := flag.Arg(0)
		searchDB(ctx, db, search)
	}

	// printDB(ctx, db)

}
