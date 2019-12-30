module github.com/jehiah/sortdb

go 1.13

require (
	github.com/bitly/timer_metrics v1.0.0
	github.com/jehiah/sortdb/src/lib/sorteddb v0.0.0-00010101000000-000000000000
)

replace github.com/jehiah/sortdb/src/lib/sorteddb => ./src/lib/sorteddb
