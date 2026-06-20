package main

import (
	"bulk-campaign/internal/metrics"
	"bulk-campaign/internal/notifier"
	"bulk-campaign/internal/pipeline"
	"bulk-campaign/internal/resilience"
	"bulk-campaign/internal/telementry"
	"bulk-campaign/internal/worker"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	workerCount = 4
	addr        = ":8080"
)

var tracer = otel.Tracer("bulk-campaign")

type server struct {
	dispatcher *notifier.Dispatcher
	logger     *slog.Logger
}

func main() {
	// Configure structured logger
	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initialize OpenTelemetry with Jaeger
	jaegerEndpoint := getEnv("JAEGER_ENDPOINT", "http://localhost:14268/api/traces")
	shutdown, err := telementry.InitTracer("bulk-campaign", jaegerEndpoint)
	if err != nil {
		logger.Error("failed to initialize tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shutdown tracer", "error", err)
		}
	}()

	dlq, err := resilience.NewDLQ(getEnv("DLQ_PATH", "./dlq.jsonl"))
	if err != nil {
		logger.Error("failed to open DLQ", "error", err)
		os.Exit(1)
	}
	defer dlq.Close()

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

	retryCfg := resilience.DefaultRetryConfig()

	s := &server{
		dispatcher: notifier.NewDispatcher(registry, retryCfg, dlq),
		logger:     logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload", s.uploadHandler)
	mux.HandleFunc("GET /healthz", healthzHandler)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Wrap handler with OpenTelemetry middleware
	handler := otelhttp.NewHandler(mux, "bulk-campaign-server")

	srv := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	go func() {
		logger.Info("server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}

	logger.Info("server stopped cleanly")
	os.Exit(0)
}

func (s *server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "upload_csv",
		trace.WithAttributes(attribute.String("component", "http.handler")),
	)
	defer span.End()

	start := time.Now()
	defer func() {
		metrics.UploadDuration.Observe(time.Since(start).Seconds())
	}()

	s.logger.InfoContext(ctx, "upload request received",
		"remote_addr", r.RemoteAddr,
		"trace_id", span.SpanContext().TraceID().String(),
	)

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		s.logger.WarnContext(ctx, "bad request", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "parse multipart failed")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		s.logger.WarnContext(ctx, "missing file field", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing file field")
		http.Error(w, "missing file field", http.StatusBadRequest)
		return
	}
	defer file.Close()

	span.SetAttributes(
		attribute.String("filename", header.Filename),
		attribute.Int64("file_size", header.Size),
	)

	s.logger.InfoContext(ctx, "processing upload",
		"filename", header.Filename,
		"size", header.Size,
	)

	records, errc := pipeline.Reader(ctx, file)

	process := func(pCtx context.Context, rec pipeline.Record) error {
		_, procSpan := tracer.Start(pCtx, "process_record")
		defer procSpan.End()

		procSpan.SetAttributes(
			attribute.String("email", rec.Email),
			attribute.String("name", rec.Name),
		)

		err := s.dispatcher.Send(pCtx, rec)
		if err != nil {
			procSpan.RecordError(err)
			procSpan.SetStatus(codes.Error, err.Error())
		} else {
			procSpan.SetStatus(codes.Ok, "")
		}
		return err
	}

	var total, failed int
	for result := range worker.Pool(ctx, workerCount, records, process) {
		total++
		if result.Err != nil {
			failed++
			metrics.RecordsProcessed.WithLabelValues("failed").Inc()
			s.logger.DebugContext(ctx, "record failed",
				"email", result.Record.Email,
				"error", result.Err,
			)
		} else {
			metrics.RecordsProcessed.WithLabelValues("success").Inc()
		}
	}

	if err := <-errc; err != nil {
		s.logger.ErrorContext(ctx, "reader error", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "reader failed")
	}

	span.SetAttributes(
		attribute.Int("total_records", total),
		attribute.Int("failed_records", failed),
		attribute.Int("success_records", total-failed),
	)

	if failed > 0 {
		span.SetStatus(codes.Error, fmt.Sprintf("%d records failed", failed))
	} else {
		span.SetStatus(codes.Ok, "")
	}

	duration := time.Since(start)
	s.logger.InfoContext(ctx, "upload complete",
		"total", total,
		"failed", failed,
		"duration_ms", duration.Milliseconds(),
		"trace_id", span.SpanContext().TraceID().String(),
	)

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
