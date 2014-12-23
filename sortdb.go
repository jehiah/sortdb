package main

import (
	"flag"
	"log"
	"os"
)

func main() {
	// showVersion := flag.Bool("version", false, "print version string")
	file := flag.String("db-file", "", "db file")
	fieldSeparator := flag.String("field-separator", "\t", "field separator (eg: comma, tab, pipe)")

	flag.Parse()

	if len(*fieldSeparator) != 1 {
		log.Fatalf("Error: invalid field separator %q", *fieldSeparator)
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("error opening %q %s", *file, err)
	}
	db, err := NewDB(f)
	if err != nil {
		log.Fatalf("error mapping %s", err)
	}
	db.recordSep = []byte(*fieldSeparator)[0]

	for _, q := range []string{"prefix.1", "e", "a", "aa", "zzzzzzzzzzzzzzzzzzzzzzzzzz"} {
		f, ok := db.Search([]byte(q))
		log.Printf("search %q found %q %v", q, f, ok)
	}
	db.Close()
}
