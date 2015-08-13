package sorted_db

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

type testCase struct {
	startIndex int
	needle     byte
	expected   int
}

func TestLastIndexByte(t *testing.T) {
	b := []byte{'a', 'b', 'c', 'd', 'e', 'a', 'b', 'c', 'd', 'e'}
	for _, tc := range []testCase{
		testCase{3, 'a', 0},
		testCase{5, 'b', 1},
		testCase{3, 'e', -1},
	} {
		i := lastIndexByte(b, tc.startIndex, tc.needle)
		if i != tc.expected {
			t.Errorf("got %d expected %d searching for %q in %q", i, tc.expected, tc.needle, b)
		}
	}
}

type testSearch struct {
	needle   string
	expected string
}

func TestSearch(t *testing.T) {
	f, err := os.Open("../test_data/testdb.tab")
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	db, err := New(f)
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	for _, tc := range []testSearch{
		{"a", "first record"},
		{"aa", "another first"},
		{"q", "r"},
		{"not found", ""},
		{"zzzzzzzzzzzzzzzzzzzzzzzzz", "very-sleepy"},
		{"zzzzzzzzzzzzzzzzzzzzzzzzzz", "already-asleep"},
	} {
		result := db.Search([]byte(tc.needle))
		if len(result) > 0 {
			result = result[len(tc.needle)+1:]
		}
		if !bytes.Equal(result, []byte(tc.expected)) {
			t.Errorf("expected %q got %q expected %q", tc.needle, result, tc.expected)
		}
	}

}

// Tests that slices returned by Search aren't modified by changes
// to the DB file afterwards.
func TestSearchWhileWriting(t *testing.T) {
	f, err := os.Open("../test_data/testdb.tab")
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	// Create a temporary copy of the DB file since we have to write to the DB
	// for this test to work
	fTmp, err := ioutil.TempFile("../test_data", "tmp_testdb")
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	defer os.Remove(fTmp.Name())
	io.Copy(fTmp, f)
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	db, err := New(fTmp)
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	tc := testSearch{"a", "first record"}
	result := db.Search([]byte(tc.needle))

	// Overwrite the temporary file with a bunch of 0s,
	// thus changing db.data (since it's mMapped to the file)
	l := len(tc.expected) + len(tc.needle) + 1
	n, err := fTmp.WriteAt(make([]byte, l), 0)
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	if n != l {
		t.Fatalf("failed to overwrite record in DB file")
	}
	if len(result) > 0 {
		result = result[len(tc.needle)+1:]
	}
	if !bytes.Equal(result, []byte(tc.expected)) {
		t.Errorf("query %q got %q expected %q", tc.needle, result, tc.expected)
	}
}

func TestSearchCharset(t *testing.T) {
	f, err := os.Open("../test_data/char_test.tsv")
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	db, err := New(f)
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	for i := 0; i <= 255; i++ {
		if i == 9 || i == 10 {
			continue
		}
		needle := []byte{byte(i)}
		result := db.Search(needle)
		if len(result) == 3 {
			result = result[2:]
		}
		if !bytes.Equal(result, needle) {
			t.Errorf("query %q got %q expected %q", needle, result, needle)
		}
	}
}
