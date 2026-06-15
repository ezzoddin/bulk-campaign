package notifier

import (
	"bulk-campaign/internal/pipeline"
	"context"
	"fmt"
)

// Dispatcher resolves the correct Notifier from a registry and delegates.
type Dispatcher struct {
	registry map[Channel]Notifier
}

// NewDispatcher builds a Dispatcher from an explicit map.
func NewDispatcher(registry map[Channel]Notifier) *Dispatcher {
	return &Dispatcher{registry: registry}
}

// Send looks up the channel stored in rec.Payload["channel"] and delegates.
// Falls back to email if the key is absent.
func (d *Dispatcher) Send(ctx context.Context, rec pipeline.Record) error {
	ch := Channel(rec.Payload["channel"])
	if ch == "" {
		ch = ChannelEmail
	}

	n, ok := d.registry[ch]
	if !ok {
		return fmt.Errorf("notifier: unknown channel %q", ch)
	}

	return n.Send(ctx, rec)
}
