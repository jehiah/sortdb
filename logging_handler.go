// largely adapted from https://github.com/gorilla/handlers/blob/master/handlers.go
// to add logging of request duration as last value (and drop referrer)

package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// responseLogger is wrapper of http.ResponseWriter that keeps track of its HTTP status
// code and body size
type responseLogger struct {
	w      http.ResponseWriter
	status int
	size   int
}

func (l *responseLogger) Header() http.Header {
	return l.w.Header()
}

func (l *responseLogger) Write(b []byte) (int, error) {
	if l.status == 0 {
		// The status will be StatusOK if WriteHeader has not been called yet
		l.status = http.StatusOK
	}
	size, err := l.w.Write(b)
	l.size += size
	return size, err
}

func (l *responseLogger) WriteHeader(s int) {
	l.w.WriteHeader(s)
	l.status = s
}

func (l *responseLogger) Status() int {
	return l.status
}

func (l *responseLogger) Size() int {
	return l.size
}

// loggingHandler is the http.Handler implementation for LoggingHandlerTo and its friends
type loggingHandler struct {
	writer  io.Writer
	handler http.Handler
}

func LoggingHandler(out io.Writer, h http.Handler) http.Handler {
	return loggingHandler{out, h}
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	t := time.Now()
	logger := &responseLogger{w: w}
	url := *req.URL
	h.handler.ServeHTTP(logger, req)
	logLine := buildLogLine(req, url, t, logger.Status(), logger.Size())
	h.writer.Write(logLine)
}

// Log entry for req similar to Apache Common Log Format.
// ts is the timestamp with which the entry should be logged.
// status, size are used to provide the response HTTP status and size.
func buildLogLine(req *http.Request, url url.URL, ts time.Time, status int, size int) []byte {
	username := "-"
	if url.User != nil {
		if name := url.User.Username(); name != "" {
			username = name
		}
	}

	host, _, err := net.SplitHostPort(req.RemoteAddr)

	if err != nil {
		host = req.RemoteAddr
	}

	duration := float64(time.Now().Sub(ts)) / float64(time.Second)

	logLine := fmt.Sprintf("%s - %s [%s] %s %q %s %d %d %q %0.3f\n",
		host,
		username,
		ts.Format("02/Jan/2006:15:04:05 -0700"),
		req.Method,
		url.RequestURI(),
		req.Proto,
		status,
		size,
		req.UserAgent(),
		duration,
	)
	return []byte(logLine)
}
