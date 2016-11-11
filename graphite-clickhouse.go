package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/lomik/graphite-clickhouse/backend"
	"github.com/lomik/zapwriter"
	"github.com/uber-go/zap"

	_ "net/http/pprof"
)

// Version of carbon-clickhouse
const Version = "0.1"

type LogResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *LogResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *LogResponseWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func WrapResponseWriter(w http.ResponseWriter) *LogResponseWriter {
	if wrapped, ok := w.(*LogResponseWriter); ok {
		return wrapped
	}
	return &LogResponseWriter{ResponseWriter: w}
}

func Handler(logger zap.Logger, handler http.Handler) http.Handler {
	var requestCounter uint32
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := WrapResponseWriter(w)

		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" {
			requestID = fmt.Sprintf("%d", atomic.AddUint32(&requestCounter, 1))
		}

		logger := logger.With(zap.String("requestID", requestID))

		r = r.WithContext(context.WithValue(r.Context(), "logger", logger))

		start := time.Now()
		handler.ServeHTTP(w, r)
		logger.Info("access",
			zap.Int("time_ms", int(time.Since(start)/time.Millisecond)),
			zap.String("method", r.Method),
			zap.String("url", r.URL.String()),
			zap.String("peer", r.RemoteAddr),
			zap.Int("status", writer.Status()),
		)
	})
}

func main() {
	var err error

	/* CONFIG start */

	configFile := flag.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")

	printVersion := flag.Bool("version", false, "Print version")

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
		return
	}

	if *printDefaultConfig {
		if err = backend.PrintConfig(backend.NewConfig()); err != nil {
			log.Fatal(err)
		}
		return
	}

	cfg := backend.NewConfig()
	if err := backend.ParseConfig(*configFile, cfg); err != nil {
		log.Fatal(err)
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}
	runtime.GOMAXPROCS(cfg.Common.MaxCPU)

	zapOutput, err := zapwriter.New(cfg.Common.LogFile)
	if err != nil {
		log.Fatal(err)
	}

	logger := zap.New(
		zapwriter.NewMixedEncoder(),
		zap.AddCaller(),
		zap.Output(zapOutput),
	)
	logger.SetLevel(cfg.Common.LogLevel)

	/* CONFIG end */

	http.Handle("/metrics/find/", Handler(logger, backend.NewFindHandler(cfg)))
	http.Handle("/render/", Handler(logger, backend.NewRenderHandler(cfg)))

	http.Handle("/", Handler(logger, http.HandlerFunc(http.NotFound)))

	log.Fatal(http.ListenAndServe(cfg.Common.Listen, nil))
}