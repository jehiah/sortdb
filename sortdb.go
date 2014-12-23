package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jehiah/sortdb/util"
)

func main() {
	// showVersion := flag.Bool("version", false, "print version string")
	file := flag.String("db-file", "", "db file")
	httpAddress := flag.String("http-address", ":8080", "http addres to listen on")
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

	ctx := &Context{
		filename:      *file,
		db:            db,
		httpAddr:      verifyAddress("http-address", *httpAddress),
		notifications: make(chan int),
	}

	hupChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	go func() {
		for {
			<-hupChan
			f, err := os.Open(*file)
			if err != nil {
				log.Fatalf("error opening %q %s", *file, err)
			}
			err = db.Reload(f)
			if err != nil {
				log.Fatalf("failed realoding file %q %s", *file, err)
			}
		}
	}()

	httpListener, err := net.Listen("tcp", ctx.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", ctx.httpAddr, err)
	}
	ctx.httpListener = httpListener
	httpServer := NewHTTPServer(ctx)

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	ctx.waitGroup.Wrap(func() {
		logger := log.New(os.Stderr, "", log.LstdFlags)
		util.HTTPServer(ctx.httpListener, httpServer, logger, "HTTP")
	})

	<-exitChan

	ctx.httpListener.Close()
	close(ctx.notifications)
	db.Close()
	ctx.waitGroup.Wait()
}
