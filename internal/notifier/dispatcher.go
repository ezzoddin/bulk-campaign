package notifier

import (
	"bulk-campaign/internal/pipeline"
	"bulk-campaign/internal/resilience"
	"context"
	"fmt"
)

// Dispatcher resolves the correct Notifier and delegates with retry + DLQ.
type Dispatcher struct {
	registry map[Channel]Notifier
	retryCfg resilience.RetryConfig
	dlq      *resilience.DLQ
}

// NewDispatcher builds a Dispatcher with retry and DLQ support.
func NewDispatcher(registry map[Channel]Notifier, retryCfg resilience.RetryConfig, dlq *resilience.DLQ) *Dispatcher {
	return &Dispatcher{
		registry: registry,
		retryCfg: retryCfg,
		dlq:      dlq,
	}
}

// Send looks up the channel, wraps in retry, and writes to DLQ on permanent failure.
func (d *Dispatcher) Send(ctx context.Context, rec pipeline.Record) error {
	ch := Channel(rec.Payload["channel"])
	if ch == "" {
		ch = ChannelEmail
	}

	n, ok := d.registry[ch]
	if !ok {
		err := fmt.Errorf("notifier: unknown channel %q", ch)
		_ = d.dlq.Write(rec, err) // log config error to DLQ
		return err
	}

	err := resilience.RetryFunc(ctx, d.retryCfg, func(ctx context.Context) error {
		return n.Send(ctx, rec)
	})

	if err != nil {
		_ = d.dlq.Write(rec, err)
		return err
	}

	return nil
}
