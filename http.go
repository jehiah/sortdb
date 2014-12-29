package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jehiah/sortdb/timer_metrics"
)

type httpServer struct {
	ctx *Context

	Requests     uint64
	GetRequests  uint64
	GetHits      uint64
	GetMisses    uint64
	MgetRequests uint64
	MgetHits     uint64
	MgetMisses   uint64

	GetMetrics  *timer_metrics.TimerMetrics
	MgetMetrics *timer_metrics.TimerMetrics
}

func NewHTTPServer(ctx *Context, logging bool) http.Handler {
	h := &httpServer{
		ctx:         ctx,
		GetMetrics:  timer_metrics.NewTimerMetrics(1500, "/get"),
		MgetMetrics: timer_metrics.NewTimerMetrics(1500, "/mget"),
	}
	if logging {
		return LoggingHandler(os.Stdout, h)
	}
	return h
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/ping":
		s.pingHandler(w, req)
	case "/get":
		s.getHandler(w, req)
	case "/mget":
		s.mgetHandler(w, req)
	// case "/fwmatch":
	// 	s.fwmatchHandler(w, req)
	case "/stats":
		s.statsHandler(w, req)
	case "/reload":
		s.reloadHandler(w, req)
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

func (s *httpServer) getHandler(w http.ResponseWriter, req *http.Request) {
	key := req.FormValue("key")
	if key == "" {
		http.Error(w, "MISSING_ARG_KEY", 400)
		return
	}
	startTime := time.Now()
	atomic.AddUint64(&s.Requests, 1)
	atomic.AddUint64(&s.GetRequests, 1)

	needle := append([]byte(key), s.ctx.db.RecordSeparator)
	line := s.ctx.db.Search(needle)

	if len(line) == 0 {
		atomic.AddUint64(&s.GetMisses, 1)
		http.Error(w, "NOT_FOUND", 404)
	} else {
		// we only output the 'value'
		line = line[len(needle):]
		atomic.AddUint64(&s.GetHits, 1)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(line)+1))
		w.Write(line)
		w.Write([]byte{s.ctx.db.LineEnding})
	}
	s.GetMetrics.Status(startTime)
}

func (s *httpServer) mgetHandler(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	if len(req.Form["key"]) == 0 {
		http.Error(w, "MISSING_ARG_KEY", 400)
		return
	}
	startTime := time.Now()
	atomic.AddUint64(&s.Requests, 1)
	atomic.AddUint64(&s.MgetRequests, 1)

	w.Header().Set("Content-Type", "text/plain")
	var numFound int
	for _, key := range req.Form["key"] {
		needle := append([]byte(key), s.ctx.db.RecordSeparator)
		line := s.ctx.db.Search(needle)
		if len(line) != 0 {
			numFound += 1
			w.Write(line)
			w.Write([]byte{s.ctx.db.LineEnding})
		}
	}
	if numFound == 0 {
		atomic.AddUint64(&s.MgetMisses, 1)
		w.WriteHeader(200)
	} else {
		atomic.AddUint64(&s.MgetHits, 1)
	}
	s.MgetMetrics.Status(startTime)
}

func (s *httpServer) reloadHandler(w http.ResponseWriter, req *http.Request) {
	s.ctx.reloadChan <- 1
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

type statsResponse struct {
	Requests     uint64        `json:"total_requests"`
	SeekCount    uint64        `json:"total_seeks"`
	GetRequests  uint64        `json:"get_requests"`
	GetHits      uint64        `json:"get_hits"`
	GetMisses    uint64        `json:"get_misses"`
	GetAvg       time.Duration `json:"get_average_request"` // Microsecond
	Get95        time.Duration `json:"get_95"`              // Microsecond
	Get99        time.Duration `json:"get_99"`              // Microsecond
	MgetRequests uint64        `json:"mget_requests"`
	MgetHits     uint64        `json:"mget_hits"`
	MgetMisses   uint64        `json:"mget_misses"`
	MgetAvg      time.Duration `json:"mget_average_request"` // Microsecond
	Mget95       time.Duration `json:"mget_95"`              // Microsecond
	Mget99       time.Duration `json:"mget_99"`              // Microsecond
}

func (s *httpServer) statsHandler(w http.ResponseWriter, req *http.Request) {
	getStats := s.GetMetrics.Stats()
	mgetStats := s.MgetMetrics.Stats()
	stats := statsResponse{
		Requests:     atomic.LoadUint64(&s.Requests),
		SeekCount:    s.ctx.db.SeekCount(),
		GetRequests:  atomic.LoadUint64(&s.GetRequests),
		GetHits:      atomic.LoadUint64(&s.GetHits),
		GetMisses:    atomic.LoadUint64(&s.GetMisses),
		GetAvg:       getStats.Avg / time.Microsecond,
		Get95:        getStats.P95 / time.Microsecond,
		Get99:        getStats.P99 / time.Microsecond,
		MgetRequests: atomic.LoadUint64(&s.MgetRequests),
		MgetHits:     atomic.LoadUint64(&s.MgetHits),
		MgetMisses:   atomic.LoadUint64(&s.MgetMisses),
		MgetAvg:      mgetStats.Avg / time.Microsecond,
		Mget95:       mgetStats.P95 / time.Microsecond,
		Mget99:       mgetStats.P99 / time.Microsecond,
	}
	// evbuffer_add_printf(evb, "\"total_seeks\": %"PRIu64",", total_seeks);

	response, err := json.Marshal(stats)
	if err != nil {
		log.Printf("%s", err)
		http.Error(w, "INTERNAL_ERROR", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(200)
	w.Write(response)

}
