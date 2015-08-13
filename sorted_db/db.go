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

// beginningOfLine locates the beginning of the line that includes i
// by searching for the last line separator occurrence before i.
func (db *DB) beginningOfLine(i int) int {
	previous := lastIndexByte(db.data, i, db.LineEnding)
	// returns the index to the first non-line-ending byte (or to the
	// beginning of the DB if no line ending is found)
	return previous + 1
}

// endOfLine locates the end of the line that includes i
// by searching for the first line separator occurrence after i.
func (db *DB) endOfLine(i int) int {
	return indexByte(db.data, i, db.size, db.LineEnding)
}

// lastIndexByte returns the index of the first instance of c in s before i. If
// c is not present in s, or -1 if c is not present in s before i.
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
func (db *DB) findFirstMatch(needle []byte, isMatch func([]byte) bool) int {
	needleLen := len(needle)

	// binary search to find the index that matches our needle,
	// starting at the previous line.
	// note: this could be more efficient if we wrote our own search as we could
	// skip data we've checked instead of checking potentially more indexes here.
	// Because page size is 4k this should hopefully matter less.
	return sort.Search(db.size, func(i int) bool {
		// find previous line starting point
		atomic.AddUint64(&db.seekCount, 1)

		previous := db.beginningOfLine(i)

		// make sure we have space before end of the buffer
		if previous+1+needleLen > db.size {
			return false
		}
		endOfKey := indexByte(db.data, previous, db.size, db.RecordSeparator)
		if endOfKey < 0 {
			endOfKey = db.endOfLine(previous)
		}
		if endOfKey < 0 {
			endOfKey = db.size
		}
		return isMatch(db.data[previous:endOfKey])
	})
}

// findStartOfRange finds the first record that is lexically equal to or
// greater than startNeedle.
// In other words, it finds the first record in the range started by
// startNeedle.
func (db *DB) findStartOfRange(startNeedle []byte) int {
	return db.findFirstMatch(startNeedle, func(key []byte) bool {
		return bytes.Compare(key, startNeedle) >= 0
	})
}

// findEndOfRange finds the first record that is lexically greater than
// endNeedle.
// In other words, it finds the first record beyond the range ended by
// endNeedle.
func (db *DB) findEndOfRange(endNeedle []byte) int {
	return db.findFirstMatch(endNeedle, func(key []byte) bool {
		return bytes.Compare(key, endNeedle) > 0
	})
}

// forwardMatchRecords gets the start and end indices of all records that
// needle forward (prefix) matches.
func (db *DB) forwardMatchRecords(needle []byte) (int, int) {
	needleLen := len(needle)

	// Get the beginning of the first record that forward matches
	startIndex := db.findFirstMatch(needle, func(key []byte) bool {
		if len(key) > needleLen {
			key = key[:needleLen]
		}
		return bytes.Compare(key, needle) >= 0
	})

	// Get the beginning of the first record that DOESN'T forward match
	endIndex := db.findFirstMatch(needle, func(key []byte) bool {
		if len(key) > needleLen {
			key = key[:needleLen]
		}
		return bytes.Compare(key, needle) > 0
	})

	return startIndex, endIndex
}

// Search uses a binary search looking for needle, and returns the full match line.
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
	previous := db.beginningOfLine(i)

	lineEnd := db.endOfLine(previous)
	// intentionally make a copy of data
	line := []byte(db.data[previous:lineEnd])
	db.RUnlock()

	if bytes.Equal(line[:len(needle)], needle) {
		return line
	}
	return nil
}

// Retrieves all records that have keys starting with needle.
func (db *DB) ForwardMatch(needle []byte) []byte {
	db.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}
	startRecord, endRecord := db.forwardMatchRecords(needle)
	if startRecord < 0 || startRecord == db.size {
		db.RUnlock()
		return nil
	}
	startIndex := db.beginningOfLine(startRecord)

	endIndex := db.size
	if endRecord >= 0 && endRecord < db.size {
		endIndex = db.beginningOfLine(endRecord)
	}
	// intentionally make a copy of data
	records := []byte(db.data[startIndex:endIndex])
	db.RUnlock()

	return records
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
	startIndex := db.beginningOfLine(startRecord)

	endIndex := db.size
	if endNeedle != nil {
		endRecord := db.findEndOfRange(endNeedle)
		if endRecord >= 0 && endRecord < db.size {
			endIndex = db.beginningOfLine(endRecord)
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
