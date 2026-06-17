package main

import (
	"bulk-campaign/internal/metrics"
	"bulk-campaign/internal/notifier"
	"bulk-campaign/internal/pipeline"
	"bulk-campaign/internal/worker"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	workerCount = 4
	addr        = ":8080"
)

type server struct {
	dispatcher *notifier.Dispatcher
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	smtpPort, _ := strconv.Atoi(getEnv("SMTP_PORT", "587"))

	emailNotifier := notifier.NewEmailNotifier(notifier.EmailConfig{
		Host:     getEnv("SMTP_HOST", "localhost"),
		Port:     smtpPort,
		Username: getEnv("SMTP_USER", ""),
		Password: getEnv("SMTP_PASS", ""),
		From:     getEnv("SMTP_FROM", "no-reply@example.com"),
	})

	registry := map[notifier.Channel]notifier.Notifier{
		notifier.ChannelEmail: emailNotifier,
	}

	s := &server{
		dispatcher: notifier.NewDispatcher(registry),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload", s.uploadHandler)
	mux.HandleFunc("GET /healthz", healthzHandler)
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}

	log.Println("server stopped cleanly")
	os.Exit(0)
}

func (s *server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		metrics.UploadDuration.Observe(time.Since(start).Seconds())
	}()

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file field", http.StatusBadRequest)
		return
	}
	defer file.Close()

	ctx := r.Context()
	records, errc := pipeline.Reader(ctx, file)

	process := func(ctx context.Context, rec pipeline.Record) error {
		return s.dispatcher.Send(ctx, rec)
	}

	var total, failed int
	for result := range worker.Pool(ctx, workerCount, records, process) {
		total++
		if result.Err != nil {
			failed++
			metrics.RecordsProcessed.WithLabelValues("failed").Inc()
			log.Printf("record error: %v", result.Err)
		} else {
			metrics.RecordsProcessed.WithLabelValues("success").Inc()
		}
	}

	if err := <-errc; err != nil {
		log.Printf("reader error: %v", err)
	}

	fmt.Fprintf(w, "done — processed %d records, %d failed\n", total, failed)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
