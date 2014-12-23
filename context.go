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
	reloadChan    chan int
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

func (c *Context) ReloadLoop() {
	for {
		<-c.reloadChan
		log.Printf("reoloading %q", c.filename)
		f, err := os.Open(c.filename)
		if err != nil {
			log.Fatalf("error opening %q %s", c.filename, err)
		}
		err = c.db.Reload(f)
		if err != nil {
			log.Fatalf("failed realoding file %q %s", c.filename, err)
		}
	}

}
