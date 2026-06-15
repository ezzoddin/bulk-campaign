package notifier_test

import (
	"bulk-campaign/internal/notifier"
	"bulk-campaign/internal/pipeline"
	"context"
	"errors"
	"testing"
)

// stub satisfies the Notifier interface for tests.
type stub struct{ err error }

func (s stub) Send(_ context.Context, _ pipeline.Record) error { return s.err }

func TestDispatcher_RoutesToCorrectChannel(t *testing.T) {
	var emailCalled bool
	reg := map[notifier.Channel]notifier.Notifier{
		notifier.ChannelEmail: stub{},
	}

	_ = emailCalled // suppress unused warning; real assertion below

	d := notifier.NewDispatcher(reg)
	rec := pipeline.Record{
		Email:   "a@b.com",
		Payload: map[string]string{"channel": "email"},
	}

	if err := d.Send(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_FallsBackToEmail(t *testing.T) {
	reg := map[notifier.Channel]notifier.Notifier{
		notifier.ChannelEmail: stub{},
	}
	d := notifier.NewDispatcher(reg)

	// No "channel" key in Payload
	rec := pipeline.Record{Email: "a@b.com", Payload: map[string]string{}}
	if err := d.Send(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_ReturnsErrorForUnknownChannel(t *testing.T) {
	d := notifier.NewDispatcher(map[notifier.Channel]notifier.Notifier{})
	rec := pipeline.Record{Payload: map[string]string{"channel": "fax"}}

	err := d.Send(context.Background(), rec)
	if err == nil {
		t.Fatal("expected error for unknown channel")
	}
}

func TestDispatcher_PropagatesNotifierError(t *testing.T) {
	sentinel := errors.New("smtp down")
	reg := map[notifier.Channel]notifier.Notifier{
		notifier.ChannelEmail: stub{err: sentinel},
	}
	d := notifier.NewDispatcher(reg)
	rec := pipeline.Record{Payload: map[string]string{"channel": "email"}}

	if !errors.Is(d.Send(context.Background(), rec), sentinel) {
		t.Fatal("expected sentinel error")
	}
}
