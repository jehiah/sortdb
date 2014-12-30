package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/jehiah/sortdb/sorted_db"
	"github.com/jehiah/sortdb/util"
)

func main() {
	showVersion := flag.Bool("version", false, "print version string")
	file := flag.String("db-file", "", "db file")
	httpAddress := flag.String("http-address", ":8080", "http address to listen on")
	fieldSeparator := flag.String("field-separator", "\t", "field separator (eg: comma, tab, pipe)")
	requestLogging := flag.Bool("enable-logging", false, "request logging")

	flag.Parse()

	if *showVersion {
		fmt.Printf("sortdb v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	if len(*fieldSeparator) != 1 {
		log.Fatalf("Error: invalid field separator %q", *fieldSeparator)
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("ERROR opening %q %s", *file, err)
	}
	db, err := sorted_db.New(f)
	if err != nil {
		log.Fatalf("ERROR creating db %s", err)
	}
	db.RecordSeparator = []byte(*fieldSeparator)[0]

	ctx := &Context{
		db:         db,
		httpAddr:   verifyAddress("http-address", *httpAddress),
		reloadChan: make(chan int),
	}

	hupChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	go func() {
		for {
			<-hupChan
			ctx.reloadChan <- 1
		}
	}()
	go ctx.ReloadLoop()

	httpListener, err := net.Listen("tcp", ctx.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", ctx.httpAddr, err)
	}
	ctx.httpListener = httpListener
	httpServer := NewHTTPServer(ctx, *requestLogging)

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
	db.Close()
	ctx.waitGroup.Wait()
}
