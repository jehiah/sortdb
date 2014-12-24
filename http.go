package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
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
}

func NewHTTPServer(ctx *Context) *httpServer {
	return &httpServer{
		ctx: ctx,
	}
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
	atomic.AddUint64(&s.Requests, 1)
	atomic.AddUint64(&s.GetRequests, 1)
	needle := append([]byte(key), s.ctx.db.RecordSeparator)
	line := s.ctx.db.Search(needle)

	if len(line) == 0 {
		atomic.AddUint64(&s.GetMisses, 1)
		http.Error(w, "NOT_FOUND", 404)
		return
	}
	atomic.AddUint64(&s.GetHits, 1)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(line)+1))
	w.Write(line)
	w.Write([]byte{s.ctx.db.LineEnding})
}

func (s *httpServer) mgetHandler(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	if len(req.Form["key"]) == 0 {
		http.Error(w, "MISSING_ARG_KEY", 400)
		return
	}
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
}

func (s *httpServer) reloadHandler(w http.ResponseWriter, req *http.Request) {
	s.ctx.reloadChan <- 1
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

type statsResponse struct {
	Requests     uint64 `json:"total_requests"`
	GetRequests  uint64 `json:"get_requests"`
	GetHits      uint64 `json:"get_hits"`
	GetMisses    uint64 `json:"get_misses"`
	MgetRequests uint64 `json:"mget_requests"`
	MgetHits     uint64 `json:"mget_hits"`
	MgetMisses   uint64 `json:"mget_misses"`
}

func (s *httpServer) statsHandler(w http.ResponseWriter, req *http.Request) {
	stats := statsResponse{
		Requests:     atomic.LoadUint64(&s.Requests),
		GetRequests:  atomic.LoadUint64(&s.GetRequests),
		GetHits:      atomic.LoadUint64(&s.GetHits),
		GetMisses:    atomic.LoadUint64(&s.GetMisses),
		MgetRequests: atomic.LoadUint64(&s.MgetRequests),
		MgetHits:     atomic.LoadUint64(&s.MgetHits),
		MgetMisses:   atomic.LoadUint64(&s.MgetMisses),
	}
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

	// for (i = 0; i < st->callback_count; i++) {
	//     evbuffer_add_printf(evb, "\"%s_95\": %"PRIu64",", st->stats_labels[i], st->ninety_five_percents[i]);
	//     evbuffer_add_printf(evb, "\"%s_average_request\": %"PRIu64",", st->stats_labels[i], st->average_requests[i]);
	//     evbuffer_add_printf(evb, "\"%s_requests\": %"PRIu64",", st->stats_labels[i], st->stats_counts[i]);
	// }
	// evbuffer_add_printf(evb, "\"get_hits\": %"PRIu64",", get_hits);
	// evbuffer_add_printf(evb, "\"get_misses\": %"PRIu64",", get_misses);
	// evbuffer_add_printf(evb, "\"fwmatch_hits\": %"PRIu64",", fwmatch_hits);
	// evbuffer_add_printf(evb, "\"fwmatch_misses\": %"PRIu64",", fwmatch_misses);
	// evbuffer_add_printf(evb, "\"total_seeks\": %"PRIu64",", total_seeks);
	// evbuffer_add_printf(evb, "\"total_requests\": %"PRIu64, st->requests);

}
