package sorteddb

import (
	"bytes"
	"sort"
	"sync/atomic"
)

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

// Copies all bytes in s to a new destination buffer
func makeCopy(s []byte) []byte {
	d := make([]byte, len(s))
	copy(d, s)
	return d
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

		startOfKey := db.beginningOfLine(i)

		// make sure we have space before end of the buffer
		if startOfKey+1+needleLen > db.size {
			return false
		}

		// The list of delimiters that can end a key
		d := [...]byte{
			db.RecordSeparator,
			db.LineEnding,
		}

		// Find the first delimiter that occurs after the start of this key by
		// searching for each delimiter and taking the minimum index that isn't -1
		endOfKey := -1
		for _, e := range d {
			i := indexByte(db.data, startOfKey, db.size, e)
			if i != -1 && (endOfKey < 0 || i < endOfKey) {
				endOfKey = i
			}
		}
		if endOfKey < 0 {
			// If no delimiter was found, just seek to the end of the DB
			endOfKey = db.size
		}
		return isMatch(db.data[startOfKey:endOfKey])
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

	// To find the range of records that forward matches, we'll perform two
	// binary searches. sort.Search is effective at finding the start of a range where
	// a given query holds true, so we'll use the following set difference to find
	// the range that we want.
	// (records where prefix >= needle) - (records where prefix > needle) =
	// (records where prefix == needle)

	// Find the first record where the prefix is equal to or greater than needle
	startIndex := db.findFirstMatch(needle, func(key []byte) bool {
		if len(key) > needleLen {
			key = key[:needleLen]
		}
		return bytes.Compare(key, needle) >= 0
	})

	// Find the first record where the prefix is STRICTLY greater than needle
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
	db.mutex.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}
	i := db.findStartOfRange(needle)
	if i < 0 || i == db.size {
		db.mutex.RUnlock()
		return nil
	}
	previous := db.beginningOfLine(i)

	lineEnd := db.endOfLine(previous)
	// copy data before unlocking to avoid race conditions
	line := makeCopy(db.data[previous:lineEnd])
	db.mutex.RUnlock()

	if len(line) > len(needle) && bytes.Equal(line[:len(needle)], needle) &&
		line[len(needle)] == db.RecordSeparator {
		return line
	}
	return nil
}

// ForwardMatch retrieves all records that have keys starting with needle.
func (db *DB) ForwardMatch(needle []byte) []byte {
	db.mutex.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}
	startRecord, endRecord := db.forwardMatchRecords(needle)
	if startRecord < 0 || startRecord == db.size {
		db.mutex.RUnlock()
		return nil
	}
	startIndex := db.beginningOfLine(startRecord)

	endIndex := db.size
	if endRecord >= 0 && endRecord < db.size {
		endIndex = db.beginningOfLine(endRecord)
	}
	// copy data before unlocking to avoid race conditions
	records := makeCopy(db.data[startIndex:endIndex])
	db.mutex.RUnlock()

	return records
}

// RangeMatch uses binary searches to look for startNeedle and (if not nil)
// endNeedle. Returns all full match lines that fall between startNeedle and
// endNeedle, inclusive.
func (db *DB) RangeMatch(startNeedle []byte, endNeedle []byte) []byte {
	db.mutex.RLock()

	if db.size <= 0 {
		panic("DB not Mapped")
	}
	if bytes.Compare(startNeedle, endNeedle) > 0 {
		// end is smaller than start, so the range is ill-defined
		db.mutex.RUnlock()
		return nil
	}
	startRecord := db.findStartOfRange(startNeedle)
	if startRecord < 0 || startRecord == db.size {
		db.mutex.RUnlock()
		return nil
	}
	startIndex := db.beginningOfLine(startRecord)

	endIndex := db.size
	endRecord := db.findEndOfRange(endNeedle)
	if endRecord >= 0 && endRecord < db.size {
		endIndex = db.beginningOfLine(endRecord)
	}
	// copy data before unlocking to avoid race conditions
	records := makeCopy(db.data[startIndex:endIndex])
	db.mutex.RUnlock()

	return records
}
