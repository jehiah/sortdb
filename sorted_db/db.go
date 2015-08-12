package sorted_db

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riobard/go-mmap"
)

type DB struct {
	sync.RWMutex
	f         *os.File
	data      mmap.Mmap
	seekCount uint64
	size      int

	RecordSeparator byte
	LineEnding      byte
}

// Create a new DB structure Opened against the specified file
func New(f *os.File) (*DB, error) {
	db := &DB{RecordSeparator: '\t', LineEnding: '\n', size: -1}
	err := db.Open(f)
	return db, err
}

// Info returns the mmaped backing file size and modification time
func (db *DB) Info() (int, time.Time) {
	db.RLock()
	defer db.RUnlock()
	if db.f == nil {
		return 0, time.Time{}
	}
	fi, _ := db.f.Stat()
	return db.size, fi.ModTime()
}

// Open the DB against a backing file
func (db *DB) Open(f *os.File) error {
	db.Lock()
	defer db.Unlock()
	if db.f != nil {
		db.close()
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := int(fi.Size())
	if size <= 0 {
		return fmt.Errorf("invalid files size %d (must be non-zero)", size)
	}
	log.Printf("DB Mmap %d bytes %s", size, f.Name())
	data, err := mmap.Map(f, 0, size, mmap.PROT_READ, mmap.MAP_FILE|mmap.MAP_SHARED)
	if err != nil {
		return err
	}
	db.f = f
	db.data = data
	db.size = size
	return nil
}

// Close and unmap the existing DB backing file
func (db *DB) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
}

// close and unmap DB w/o locking
func (db *DB) close() {
	if db.data != nil {
		var name string
		if db.f != nil {
			name = db.f.Name()
		}
		log.Printf("DB Unmmap %d bytes %s", db.size, name)
		db.data.Unmap()
		db.data = nil
	}
	if db.f != nil {
		log.Printf("Closing file %s", db.f.Name())
		db.f.Close()
		db.f = nil
	}
	db.size = -1
}

// Remap DB to the same backing file
func (db *DB) Remap() error {
	db.RLock()
	if db.f == nil {
		db.RUnlock()
		return fmt.Errorf("DB must be open to remap")
	}
	filename := db.f.Name()
	db.RUnlock()

	log.Printf("DB Remapping %s", filename)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	err = db.Open(f)
	if err != nil {
		return err
	}
	return nil
}

// LastIndexByte returns the index of the first instance of c in s, or -1 if c is not present in s after start.
func lastIndexByte(s []byte, i int, c byte) int {
	for ; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// IndexByte returns the index of the first instance of c in s after i but before m. If c is not present in s -1 is returned
func indexByte(s []byte, i, m int, c byte) int {
	for ; i < m; i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func (db *DB) findFirstRecord(needle []byte) int {
	needleLen := len(needle)

	// binary search to find the index that matches our needle (starting at the previous line)
	// note: this could be more efficient if we wrote our own search as we could skip data we've checked
	// isntead of checking potentially more indexes here. Because page sizes is 4k this should hopefully
	// matter less
	return sort.Search(db.size, func(i int) bool {
		// find previous line starting point
		atomic.AddUint64(&db.seekCount, 1)
		previous := lastIndexByte(db.data, i, db.LineEnding)
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
}

// Search uses a binary search looking for needle, and returns the full match line.
// the needle should already have the record separator appended
func (db *DB) Search(needle []byte) []byte {
	db.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}
	i := db.findFirstRecord(needle)
	if i < 0 || i == db.size {
		db.RUnlock()
		return nil
	}
	previous := lastIndexByte(db.data, i, db.LineEnding)
	if previous == -1 {
		previous = 0
	} else {
		previous++ // eat the line ending
	}
	lineEnd := indexByte(db.data, previous, db.size, db.LineEnding)
	// intentionally make a copy of data
	line := []byte(db.data[previous:lineEnd])
	db.RUnlock()

	if bytes.Equal(line[:len(needle)], needle) {
		return line
	}
	return nil
}

func (db *DB) SeekCount() uint64 {
	return atomic.LoadUint64(&db.seekCount)
}
