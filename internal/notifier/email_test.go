package notifier_test

import (
	"bulk-campaign/internal/notifier"
	"bulk-campaign/internal/pipeline"
	"context"
	"testing"
)

func TestEmailNotifier_RejectsEmptyEmail(t *testing.T) {
	n := notifier.NewEmailNotifier(notifier.EmailConfig{
		Host: "localhost", Port: 1025,
		From: "no-reply@example.com",
	})

	err := n.Send(context.Background(), pipeline.Record{Name: "Alice", Email: ""})
	if err == nil {
		t.Fatal("expected error for empty email")
	}
}

func TestEmailNotifier_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n := notifier.NewEmailNotifier(notifier.EmailConfig{
		Host: "localhost", Port: 1025,
		From: "no-reply@example.com",
	})

	err := n.Send(ctx, pipeline.Record{Email: "a@b.com"})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
