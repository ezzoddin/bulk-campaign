package worker

import (
	"bulk-campaign/internal/pipeline"
	"context"
	"sync"
)

// ProcessFn is the unit of work applied to every record.
type ProcessFn func(ctx context.Context, rec pipeline.Record) error

// Result pairs a record with the error produced while processing it.
type Result struct {
	Record pipeline.Record
	Err    error
}

// Pool fans out records across n goroutines, each calling fn.
// The returned channel is closed when all workers finish.
func Pool(
	ctx context.Context,
	n int,
	records <-chan pipeline.Record,
	fn ProcessFn,
) <-chan Result {
	results := make(chan Result, n)

	var wg sync.WaitGroup
	wg.Add(n)

	for range n {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case rec, ok := <-records:
					if !ok {
						return
					}
					results <- Result{Record: rec, Err: fn(ctx, rec)}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}
