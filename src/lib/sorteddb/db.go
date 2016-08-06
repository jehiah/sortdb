package sorteddb

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riobard/go-mmap"
)

type DB struct {
	RecordSeparator byte
	LineEnding      byte

	f         *os.File
	data      mmap.Mmap
	seekCount uint64
	size      int
	mlock     bool

	mutex sync.RWMutex
}

// Create a new DB structure Opened against the specified file
func New(f *os.File) (*DB, error) {
	db := &DB{RecordSeparator: '\t', LineEnding: '\n', size: -1}
	err := db.Open(f)
	return db, err
}

// Info returns the mmaped backing file size and modification time
func (db *DB) Info() (int, time.Time) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	if db.f == nil {
		return 0, time.Time{}
	}
	fi, _ := db.f.Stat()
	return db.size, fi.ModTime()
}

// Open the DB against a backing file
func (db *DB) Open(f *os.File) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
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
	if db.mlock {
		data.Lock()
	}
	return nil
}

// Close and unmap the existing DB backing file
// If Mlocked, data will be munlocked
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.close()
}

// close and unmap DB w/o locking
func (db *DB) close() (err error) {
	if db.data != nil {
		if db.mlock {
			db.munlock()
		}
		var name string
		if db.f != nil {
			name = db.f.Name()
		}
		log.Printf("DB Unmmap %d bytes %s", db.size, name)
		err = db.data.Unmap()
		db.data = nil
	}
	if db.f != nil {
		log.Printf("Closing file %s", db.f.Name())
		db.f.Close()
		db.f = nil
	}
	db.size = -1
	return
}

// Remap DB to the same backing file
func (db *DB) Remap() error {
	db.mutex.RLock()
	if db.f == nil {
		db.mutex.RUnlock()
		return fmt.Errorf("DB must be open to remap")
	}
	filename := db.f.Name()
	db.mutex.RUnlock()

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

// Mlock prevent the mmap from being paged to the swap area.
func (db *DB) Mlock() error {
	db.mutex.Lock()
	if db.data == nil {
		return fmt.Errorf("db not open")
	}
	defer db.mutex.Unlock()
	db.mlock = true
	return db.data.Lock()
}

// Munlock calls syscall.Munlock on the underlying data if already Mlock'd
func (db *DB) Munlock() error {
	db.mutex.Lock()
	if db.data == nil {
		return fmt.Errorf("db not open")
	}
	defer db.mutex.Unlock()
	return db.munlock()
}

func (db *DB) munlock() error {
	db.mlock = false
	return db.data.Unlock()
}

func (db *DB) SeekCount() uint64 {
	return atomic.LoadUint64(&db.seekCount)
}
