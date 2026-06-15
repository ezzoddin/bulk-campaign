package notifier

import (
	"bulk-campaign/internal/pipeline"
	"context"
	"fmt"

	"gopkg.in/gomail.v2"
)

// EmailConfig holds SMTP connection parameters.
type EmailConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

// EmailNotifier sends transactional email via SMTP.
type EmailNotifier struct {
	cfg    EmailConfig
	dialer *gomail.Dialer
}

// NewEmailNotifier constructs an EmailNotifier and opens a reusable dialer.
func NewEmailNotifier(cfg EmailConfig) *EmailNotifier {
	d := gomail.NewDialer(cfg.Host, cfg.Port, cfg.Username, cfg.Password)
	return &EmailNotifier{cfg: cfg, dialer: d}
}

// Send composes and delivers a message to rec.Email.
func (e *EmailNotifier) Send(ctx context.Context, rec pipeline.Record) error {
	// Respect cancellation before doing network I/O
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if rec.Email == "" {
		return fmt.Errorf("email notifier: empty recipient for record %q", rec.Name)
	}

	//TODO: set dynamic subject and body filed
	m := gomail.NewMessage()
	m.SetHeader("From", e.cfg.From)
	m.SetHeader("To", rec.Email)
	m.SetHeader("Subject", "Your campaign message")
	m.SetBody("text/plain", fmt.Sprintf("Hello %s,\n\nThis is your campaign message.", rec.Name))

	if err := e.dialer.DialAndSend(m); err != nil {
		return fmt.Errorf("email notifier: send to %s: %w", rec.Email, err)
	}

	return nil
}
