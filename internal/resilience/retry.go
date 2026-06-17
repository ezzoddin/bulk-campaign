package resilience

import (
	"context"
	"fmt"
	"time"
)

// RetryConfig holds backoff parameters.
type RetryConfig struct {
	MaxRetries  int
	InitialWait time.Duration
	MaxWait     time.Duration
	Multiplier  float64
}

// DefaultRetryConfig provides sensible production defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  3,
		InitialWait: 100 * time.Millisecond,
		MaxWait:     5 * time.Second,
		Multiplier:  2.0,
	}
}

// RetryFunc wraps fn with exponential backoff. Returns the final error after exhausting retries.
func RetryFunc(ctx context.Context, cfg RetryConfig, fn func(context.Context) error) error {
	var lastErr error
	wait := cfg.InitialWait

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}

			wait = time.Duration(float64(wait) * cfg.Multiplier)
			if wait > cfg.MaxWait {
				wait = cfg.MaxWait
			}
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
	}

	return fmt.Errorf("retry exhausted after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}
