#!/bin/bash
set -e

pushd src/lib/sorteddb 2>/dev/null >/dev/null
go test -timeout 60s ./...
popd 2>/dev/null >/dev/null
# gb test -timeout 60s -race

go test -timeout 60s ./...
go build ./src/cmd/sortdb
