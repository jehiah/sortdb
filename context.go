package main

import (
	"log"
	"net"
	"os"

	"github.com/jehiah/sortdb/util"
)

type Context struct {
	filename      string
	db            *DB
	httpAddr      *net.TCPAddr
	httpListener  net.Listener
	notifications chan int
	waitGroup     util.WaitGroupWrapper
}

func verifyAddress(arg string, address string) *net.TCPAddr {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		log.Fatalf("FATAL: failed to resolve %s address (%s) - %s", arg, address, err)
		os.Exit(1)
	}

	return addr
}
