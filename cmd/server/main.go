package main

import (
	"bulk-campaign/internal/pipeline"
	"bulk-campaign/internal/worker"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	workerCount = 4
	addr        = ":8080"
)

func main() {
	// Root context — cancelled on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload", uploadHandler)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start HTTP server in background
	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Block until signal
	<-ctx.Done()
	log.Println("shutdown signal received")

	// Give in-flight requests 30 s to finish
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}

	log.Println("server stopped cleanly")
	os.Exit(0)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	// 32 MB max memory for multipart
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

	ctx := r.Context() // cancelled if client disconnects or server shuts down

	records, errc := pipeline.Reader(ctx, file)

	process := func(_ context.Context, rec pipeline.Record) error {
		log.Printf("processing: name=%s email=%s", rec.Name, rec.Email)
		return nil
	}

	var total, failed int
	for result := range worker.Pool(ctx, workerCount, records, process) {
		total++
		if result.Err != nil {
			failed++
			log.Printf("record error: %v", result.Err)
		}
	}

	if err := <-errc; err != nil {
		log.Printf("reader error: %v", err)
	}

	fmt.Fprintf(w, "done — processed %d records, %d failed\n", total, failed)
}
