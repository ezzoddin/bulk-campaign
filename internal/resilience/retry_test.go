package resilience_test

import (
	"bulk-campaign/internal/resilience"
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryFunc_SucceedsOnFirstAttempt(t *testing.T) {
	var calls int
	fn := func(context.Context) error {
		calls++
		return nil
	}

	cfg := resilience.RetryConfig{
		MaxRetries:  3,
		InitialWait: 10 * time.Millisecond,
		MaxWait:     100 * time.Millisecond,
		Multiplier:  2.0,
	}

	err := resilience.RetryFunc(context.Background(), cfg, fn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryFunc_RetriesUntilSuccess(t *testing.T) {
	var calls int
	fn := func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}

	cfg := resilience.RetryConfig{
		MaxRetries:  3,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	err := resilience.RetryFunc(context.Background(), cfg, fn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryFunc_ExhaustsRetries(t *testing.T) {
	var calls int
	fn := func(context.Context) error {
		calls++
		return errors.New("permanent")
	}

	cfg := resilience.RetryConfig{
		MaxRetries:  2,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}

	err := resilience.RetryFunc(context.Background(), cfg, fn)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if calls != 3 { // initial + 2 retries
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryFunc_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fn := func(context.Context) error {
		return errors.New("should not reach here")
	}

	cfg := resilience.RetryConfig{
		MaxRetries:  3,
		InitialWait: 100 * time.Millisecond,
		MaxWait:     1 * time.Second,
		Multiplier:  2.0,
	}

	err := resilience.RetryFunc(ctx, cfg, fn)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetryFunc_CapsWaitAtMaxWait(t *testing.T) {
	var calls int
	fn := func(context.Context) error {
		calls++
		return errors.New("fail")
	}

	cfg := resilience.RetryConfig{
		MaxRetries:  5,
		InitialWait: 1 * time.Millisecond,
		MaxWait:     5 * time.Millisecond,
		Multiplier:  10.0, // grows fast
	}

	start := time.Now()
	_ = resilience.RetryFunc(context.Background(), cfg, fn)
	elapsed := time.Since(start)

	// With capping, total wait should be roughly 5ms * 5 attempts = ~25ms
	// Without capping, it would explode exponentially
	if elapsed > 100*time.Millisecond {
		t.Fatalf("backoff not capped: took %v", elapsed)
	}
}
