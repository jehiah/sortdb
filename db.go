package main

import (
	"bytes"
	"os"
	"sort"
	"sync"

	"github.com/riobard/go-mmap"
)

type DB struct {
	sync.RWMutex
	f    *os.File
	data mmap.Mmap
	size int
}

func NewDB(f *os.File) (*DB, error) {
	db := &DB{}
	err := db.Open(f)
	return db, err
}

func (db *DB) Open(f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := int(fi.Size())
	data, err := mmap.Map(f, 0, size, mmap.PROT_READ, mmap.MAP_FILE|mmap.MAP_SHARED)
	if err != nil {
		return err
	}
	db.f = f
	db.data = data
	db.size = size
	return nil
}

func (db *DB) Close() {
	db.data.Unmap()
	db.f.Close()
	db.f = nil
}

func (db *DB) Reopen(f *os.File) {
	db.Lock()
	db.Close()
	db.Open(f)
	db.Unlock()
}

// LastIndexByte returns the index of the first instance of c in s, or -1 if c is not present in s after start.
func LastIndexByte(s []byte, i int, c byte) int {
	for ; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// IndexByte returns the index of the first instance of c in s after i but before m. If c is not present in s -1 is returned
func IndexByte(s []byte, i, m int, c byte) int {
	for ; i < m; i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// Search uses a binary search looking for needle based
func (db *DB) Search(needle []byte, newline, recordSep byte) ([]byte, bool) {
	// Binary search to find a matching byte slice.
	needle = append(needle, recordSep)

	needleLen := len(needle)
	i := sort.Search(db.size, func(i int) bool {
		// find previous line starting point
		previous := LastIndexByte(db.data, i, newline)
		if previous == -1 {
			return false
		}
		// make sure we have space before end of the buffer
		if previous+1+needleLen > db.size {
			return false
		}
		return bytes.Compare(db.data[previous+1:previous+1+needleLen], needle) >= 0
	})
	if i < 0 {
		return nil, false
	}
	previous := LastIndexByte(db.data, i, newline)
	lineEnd := IndexByte(db.data, previous+1, db.size, newline)
	return []byte(db.data[previous+1+needleLen : lineEnd]), true

	// re-check equals?
	// if i+needleLen < db.size && bytes.Equal(db.data[i:i+needleLen], needle) {
	// 	previous := LastIndexByte(db.data, i, '\n')
	// 	lineEnd := IndexByte(db.data, i, db.size, '\n')
	// 	return []byte(db.data[i:lineEnd]), true
	// }
	// return nil, false
}
