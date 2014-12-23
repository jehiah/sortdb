package main

import (
	"bytes"
	"log"
	"os"

	"github.com/riobard/go-mmap"
)

func Map(f *os.File) (mmap.Mmap, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	m, err := mmap.Map(f, 0, int(fi.Size()), mmap.PROT_READ, mmap.MAP_FILE|mmap.MAP_SHARED)
	return m, err
}

func main() {
	f, err := os.Open("test_data/testdb.tab")
	if err != nil {
		log.Fatalf("error opening %s", err)
	}
	data, err := Map(f)
	if err != nil {
		log.Fatalf("error mapping %s", err)
	}
	end := bytes.Index(data, []byte("\n"))
	println(string([]byte(data[:end])))
}
