package main

import (
	"io"
	"log"
	"net/http"
)

type httpServer struct {
	ctx      *Context
	counters map[string]map[string]int64
}

func NewHTTPServer(ctx *Context) *httpServer {
	return &httpServer{
		ctx:      ctx,
		counters: make(map[string]map[string]int64),
	}
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/ping":
		s.pingHandler(w, req)
	// case "/get":
	// 	s.getHandler(w, req)
	// case "/mgetHandler":
	// 	s.mgetHandler(w, req)
	// case "/fwmatch":
	// 	s.fwmatchHandler(w, req)
	// case "/stats":
	// 	s.statsHandler(w, req)
	// case "/reload":
	// 	s.reloadHandler(w, req)
	// case "/exit":
	// 	s.exitHandler(w, req)
	default:
		log.Printf("ERROR: 404 %s", req.URL.Path)
		http.NotFound(w, req)
	}
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}
