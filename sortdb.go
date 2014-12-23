package main

import (
	"bytes"
	"log"
	"os"

	"github.com/riobard/go-mmap"
)

type DB struct {
	f    *os.File
	data mmap.Mmap
	size int64
}

func OpenDB(f *os.File) (*DB, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	data, err := mmap.Map(f, 0, int(size), mmap.PROT_READ, mmap.MAP_FILE|mmap.MAP_SHARED)
	db := &DB{
		f:    f,
		data: data,
		size: size,
	}
	return db, err
}
func (db *DB) Close() {
	db.data.Unmap()
	db.f.Close()
	db.f = nil
}

func main() {
	f, err := os.Open("test_data/testdb.tab")
	if err != nil {
		log.Fatalf("error opening %s", err)
	}
	db, err := OpenDB(f)
	if err != nil {
		log.Fatalf("error mapping %s", err)
	}
	end := bytes.Index(db.data, []byte("\n"))
	record := []byte(db.data[:end])
	log.Printf("record is %q", record)
	db.Close()
}
