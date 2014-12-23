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

	recordSep  byte
	lineEnding byte
}

func NewDB(f *os.File) (*DB, error) {
	db := &DB{recordSep: '\t', lineEnding: '\n'}
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

func (db *DB) Reload(f *os.File) error {
	db.Lock()
	db.Close()
	err := db.Open(f)
	db.Unlock()
	if err != nil {
		return err
	}
	return nil
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

// Search uses a binary search looking for needle, and returns the full match line
func (db *DB) Search(needle []byte) []byte {
	db.RLock()

	needleLen := len(needle)

	// binary search to find the index that matches our needle (starting at the previous line)
	// note: this could be more efficient if we wrote our own search as we could skip data we've checked
	// isntead of checking potentially more indexes here. Because page sizes is 4k this should hopefully
	// matter less
	i := sort.Search(db.size, func(i int) bool {
		// find previous line starting point
		previous := LastIndexByte(db.data, i, db.lineEnding)
		if previous == -1 {
			previous = 0
		} else {
			previous++ // eat the line ending
		}
		// make sure we have space before end of the buffer
		if previous+1+needleLen > db.size {
			return false
		}
		return bytes.Compare(db.data[previous:previous+needleLen], needle) >= 0
	})
	if i < 0 || i == db.size {
		db.RUnlock()
		return nil
	}
	previous := LastIndexByte(db.data, i, db.lineEnding)
	if previous == -1 {
		previous = 0
	} else {
		previous++ // eat the line ending
	}
	lineEnd := IndexByte(db.data, previous, db.size, db.lineEnding)
	// intentionally make a copy of data
	line := []byte(db.data[previous:lineEnd])
	db.RUnlock()

	if bytes.Equal(line[:len(needle)], needle) {
		return line
	}
	return nil
}
