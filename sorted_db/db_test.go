package sorted_db

import (
	"bytes"
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
			t.Errorf("query %q got %q expected %q", tc.needle, result, tc.expected)
		}
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

func TestForwardMatch(t *testing.T) {
	f, err := os.Open("../test_data/testdb.tab")
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	db, err := New(f)
	if err != nil {
		t.Fatalf("got error %s", err)
	}

	for _, tc := range []testSearch{
		{"prefix", `prefix.1	how
prefix.2	are
prefix.3	you
q	r
s	t
u	v
w	x
y	z
zzzzzzzzzzzzzzzzzzzzzzzz	almost-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzz	very-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzzz	already-asleep
`},
		{"y", `y	z
zzzzzzzzzzzzzzzzzzzzzzzz	almost-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzz	very-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzzz	already-asleep
`},

		{"y1", `zzzzzzzzzzzzzzzzzzzzzzzz	almost-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzz	very-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzzz	already-asleep
`},

		{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", ""},
	} {
		actualRecords := db.RangeMatch([]byte(tc.needle), nil)
		expectedRecords := []byte(tc.expected)

		if bytes.Compare(expectedRecords, actualRecords) != 0 {
			t.Errorf("for forward match from %q:\nExpected %q but got %q", tc.needle, expectedRecords, actualRecords)
		}
	}
}

type testRangeSearch struct {
	startNeedle string
	endNeedle   string
	expected    string
}

func TestRangeMatch(t *testing.T) {
	f, err := os.Open("../test_data/testdb.tab")
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	db, err := New(f)
	if err != nil {
		t.Fatalf("got error %s", err)
	}

	for _, tc := range []testRangeSearch{
		{"0", "9", ""},
		{"0", "c1", `a	first record
aa	another first
b	third
c	d
`},
		{"b", "c1", `b	third
c	d
`},
		{"c", "b", ""},
		{"p", "prefix.3", `prefix.1	how
prefix.2	are
prefix.3	you
`},
		{"prefix.11", "prefix.3", `prefix.2	are
prefix.3	you
`},
		{"y", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", `y	z
zzzzzzzzzzzzzzzzzzzzzzzz	almost-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzz	very-sleepy
zzzzzzzzzzzzzzzzzzzzzzzzzz	already-asleep
`},
		{"y", "z", "y	z\n"},

		{"y1", "zzzzzzzzzzzzzzzzzzzzzzzz", `zzzzzzzzzzzzzzzzzzzzzzzz	almost-sleepy
`},

		{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", ""},
	} {
		actualRecords := db.RangeMatch([]byte(tc.startNeedle), []byte(tc.endNeedle))
		expectedRecords := []byte(tc.expected)

		if bytes.Compare(expectedRecords, actualRecords) != 0 {
			t.Errorf("for forward match from %q to %q:\nExpected %q but got %q", tc.startNeedle, tc.endNeedle, expectedRecords, actualRecords)
		}
	}
}
