module github.com/jehiah/sortdb

go 1.18

require (
	github.com/bitly/timer_metrics v1.0.0
	github.com/jehiah/sortdb/src/lib/sorteddb v0.0.0-00010101000000-000000000000
)

require github.com/riobard/go-mmap v0.0.0-20140328143229-8eec19e37d25 // indirect

replace github.com/jehiah/sortdb/src/lib/sorteddb => ./src/lib/sorteddb
