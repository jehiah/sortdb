package main

import (
	"log"
	"net"
	"os"

	"github.com/jehiah/sortdb/sorted_db"
	"github.com/jehiah/sortdb/util"
)

type Context struct {
	db           *sorted_db.DB
	httpAddr     *net.TCPAddr
	httpListener net.Listener
	reloadChan   chan int
	waitGroup    util.WaitGroupWrapper
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
		err := c.db.Remap()
		if err != nil {
			log.Fatalf("ERROR remapping DB %q", err)
		}
	}

}
