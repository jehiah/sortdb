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

// lastIndexByte returns the index of the first instance of c in s before i. If
// c is not present in s,or -1 if c is not present in s before i, then -1 is
// returned.
func lastIndexByte(s []byte, i int, c byte) int {
	for ; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// indexByte returns the index of the first instance of c in s after i but
// before m. If c is not present in s between i and m, then -1 is returned.
func indexByte(s []byte, i, m int, c byte) int {
	for ; i < m; i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// findFirstMatch performs a binary search to find the first record
// that matches needle using the given isMatch function, or -1 if
// no match is found.
func (db *DB) findFirstMatch(needle []byte, isMatch func([]byte, []byte) bool) int {
	needleLen := len(needle)

	// binary search to find the index that matches our needle,
	// starting at the previous line.
	// note: this could be more efficient if we wrote our own search as we could
	// skip data we've checked instead of checking potentially more indexes here.
	// Because page size is 4k this should hopefully matter less.
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
		endOfKey := indexByte(db.data, previous, db.size, db.RecordSeparator)
		if endOfKey < 0 {
			endOfKey = indexByte(db.data, previous, db.size, db.LineEnding)
		}
		if endOfKey < 0 {
			endOfKey = db.size
		}

		return isMatch(db.data[previous:endOfKey], needle)
	})
}

// findStartOfRange finds the first record that is lexically equal to or
// greater than startNeedle.
// In other words, it finds the first record in the range started by
// startNeedle.
func (db *DB) findStartOfRange(startNeedle []byte) int {
	return db.findFirstMatch(startNeedle, func(a []byte, b []byte) bool {
		return bytes.Compare(a, b) >= 0
	})
}

// findStartOfRange finds the first record that is lexically greater than
// startNeedle.
// In other words, it finds the first record beyond the range ended by
// endNeedle.
func (db *DB) findEndOfRange(endNeedle []byte) int {
	return db.findFirstMatch(endNeedle, func(a []byte, b []byte) bool {
		return bytes.Compare(a, b) > 0
	})
}

// Search uses a binary search looking for needle, and returns the full match line.
// needle should already end with the record separator.
func (db *DB) Search(needle []byte) []byte {
	db.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}
	i := db.findStartOfRange(needle)
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

// RangeMatch uses binary searches to look for startNeedle and (if not nil)
// endNeedle. Returns all full match lines that fall between startNeedle and
// endNeedle, inclusive.
func (db *DB) RangeMatch(startNeedle []byte, endNeedle []byte) []byte {
	db.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}

	if endNeedle != nil && bytes.Compare(startNeedle, endNeedle) > 0 {
		// end is smaller than start, so the range is ill-defined
		db.RUnlock()
		return nil
	}

	startRecord := db.findStartOfRange(startNeedle)
	if startRecord < 0 || startRecord == db.size {
		db.RUnlock()
		return nil
	}
	previous := lastIndexByte(db.data, startRecord, db.LineEnding)
	// eat the line ending or (if there is no line ending) move to the start
	startIndex := previous + 1

	endIndex := db.size
	if endNeedle != nil {
		endRecord := db.findEndOfRange(endNeedle)
		if endRecord >= 0 && endRecord < db.size {
			previous := lastIndexByte(db.data, endRecord, db.LineEnding)
			// eat the line ending or (if there is no line ending) move to the start
			endIndex = previous + 1
		}
	}

	// intentionally make a copy of data
	records := []byte(db.data[startIndex:endIndex])
	db.RUnlock()

	return records
}

func (db *DB) SeekCount() uint64 {
	return atomic.LoadUint64(&db.seekCount)
}
