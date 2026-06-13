package worker_test

import (
	"bulk-campaign/internal/pipeline"
	"bulk-campaign/internal/worker"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

func sendRecords(n int) <-chan pipeline.Record {
	ch := make(chan pipeline.Record, n)
	for i := range n {
		ch <- pipeline.Record{Name: fmt.Sprintf("user%d", i), Email: "a@b.com"}
	}
	close(ch)
	return ch
}

func TestPool_ProcessesAllRecords(t *testing.T) {
	var count atomic.Int64
	fn := func(_ context.Context, _ pipeline.Record) error {
		count.Add(1)
		return nil
	}

	results := worker.Pool(context.Background(), 4, sendRecords(50), fn)
	for range results {
	}

	if count.Load() != 50 {
		t.Fatalf("expected 50, got %d", count.Load())
	}
}

func TestPool_CollectsErrors(t *testing.T) {
	sentinel := errors.New("boom")
	fn := func(_ context.Context, _ pipeline.Record) error { return sentinel }

	var errCount int
	for r := range worker.Pool(context.Background(), 2, sendRecords(10), fn) {
		if r.Err != nil {
			errCount++
		}
	}

	if errCount != 10 {
		t.Fatalf("expected 10 errors, got %d", errCount)
	}
}

func TestPool_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// infinite stream — pool must still return
	ch := make(chan pipeline.Record)
	defer close(ch)

	for range worker.Pool(ctx, 2, ch, func(_ context.Context, _ pipeline.Record) error { return nil }) {
	}
}
