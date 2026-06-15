package notifier

import (
	"bulk-campaign/internal/pipeline"
	"context"
)

// Notifier is the strategy contract every channel must satisfy.
type Notifier interface {
	Send(ctx context.Context, rec pipeline.Record) error
}

// Channel identifies which delivery channel to use.
type Channel string

const (
	ChannelEmail    Channel = "email"
	ChannelSMS      Channel = "sms"
	ChannelTelegram Channel = "telegram"
)
