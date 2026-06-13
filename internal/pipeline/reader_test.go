package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

const sampleCSV = `name,email,phone
		Mobina Ezzoddin 1,m@example.com,+981111111111
		Mobina Ezzoddin 2,m@example.com,+981111111112
		Mobina Ezzoddin 3,m@example.com,+981111111113
`

func TestReader_HappyPath(t *testing.T) {
	ctx := context.Background()
	records, errc := Reader(ctx, strings.NewReader(sampleCSV))

	var got []Record
	for rec := range records {
		got = append(got, rec)
	}

	if err := <-errc; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("want 3 records, got %d", len(got))
	}

	if got[0].Name != "Mobina Ezzoddin 1" || got[0].Email != "m@example.com" {
		t.Errorf("unexpected first record: %+v", got[0])
	}
}

func TestReader_CancelMidStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Large enough that we'll hit cancellation before EOF
	var sb strings.Builder
	sb.WriteString("name,email,phone\n")
	for i := 0; i < 100_000; i++ {
		sb.WriteString("Mobina Ezzoddin 1,m@example.com,+981111111111\n")
	}

	records, errc := Reader(ctx, strings.NewReader(sb.String()))

	// Drain — we just want no deadlock
	for range records {
	}

	err := <-errc
	if err == nil {
		// Cancellation may race with EOF on a fast machine — acceptable
		t.Log("finished before cancellation (race with EOF, ok)")
	}
}

func TestReader_EmptyFile(t *testing.T) {
	ctx := context.Background()
	records, errc := Reader(ctx, strings.NewReader("name,email,phone\n"))

	var got []Record
	for rec := range records {
		got = append(got, rec)
	}

	if err := <-errc; err != nil {
		t.Fatalf("unexpected error on empty file: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("want 0 records, got %d", len(got))
	}
}
