package notifier

import (
	"bulk-campaign/internal/metrics"
	"bulk-campaign/internal/pipeline"
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"gopkg.in/mail.v2"
)

var tracer = otel.Tracer("notifier")

// EmailNotifier sends notifications via SMTP.
type EmailNotifier struct {
	cfg    EmailConfig
	logger *slog.Logger
}

// EmailConfig holds SMTP connection parameters.
type EmailConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

// NewEmailNotifier creates an SMTP-based notifier.
func NewEmailNotifier(cfg EmailConfig) *EmailNotifier {
	return &EmailNotifier{
		cfg:    cfg,
		logger: slog.Default().With("notifier", "email"),
	}
}

// Send delivers an email using SMTP.
func (e *EmailNotifier) Send(ctx context.Context, rec pipeline.Record) error {
	ctx, span := tracer.Start(ctx, "send_email")
	defer span.End()

	span.SetAttributes(
		attribute.String("email", rec.Email),
		attribute.String("smtp_host", e.cfg.Host),
	)

	if rec.Email == "" {
		err := fmt.Errorf("email: missing recipient email")
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing email")
		return err
	}

	m := mail.NewMessage()
	m.SetHeader("From", e.cfg.From)
	m.SetHeader("To", rec.Email)
	m.SetHeader("Subject", fmt.Sprintf("Hello %s", rec.Name))
	m.SetBody("text/plain", fmt.Sprintf("Hi %s, this is a bulk campaign message.", rec.Name))

	d := mail.NewDialer(e.cfg.Host, e.cfg.Port, e.cfg.Username, e.cfg.Password)

	if err := d.DialAndSend(m); err != nil {
		e.logger.ErrorContext(ctx, "failed to send email", "email", rec.Email, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("email: %w", err)
	}

	metrics.NotificationsSent.WithLabelValues("email").Inc()
	span.SetStatus(codes.Ok, "")
	e.logger.DebugContext(ctx, "email sent", "email", rec.Email)
	return nil
}
