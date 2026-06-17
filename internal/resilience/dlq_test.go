package resilience_test

import (
	"bulk-campaign/internal/pipeline"
	"bulk-campaign/internal/resilience"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestDLQ_WritesFailedRecords(t *testing.T) {
	tmp := t.TempDir()
	path := tmp + "/dlq.jsonl"

	dlq, err := resilience.NewDLQ(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()

	rec := pipeline.Record{
		Email:   "fail@example.com",
		Name:    "Failure",
		Payload: map[string]string{"channel": "email"},
	}

	if err := dlq.Write(rec, errors.New("smtp timeout")); err != nil {
		t.Fatal(err)
	}

	if err := dlq.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}

	var fr resilience.FailedRecord
	if err := json.Unmarshal([]byte(lines[0]), &fr); err != nil {
		t.Fatal(err)
	}

	if fr.Record.Email != "fail@example.com" {
		t.Fatalf("expected fail@example.com, got %s", fr.Record.Email)
	}
	if fr.Error != "smtp timeout" {
		t.Fatalf("expected 'smtp timeout', got %s", fr.Error)
	}
	if fr.Timestamp.IsZero() {
		t.Fatal("timestamp is zero")
	}
}

func TestDLQ_AppendMultipleRecords(t *testing.T) {
	tmp := t.TempDir()
	path := tmp + "/dlq.jsonl"

	dlq, err := resilience.NewDLQ(path)
	if err != nil {
		t.Fatal(err)
	}

	rec1 := pipeline.Record{Email: "a@b.com", Name: "A"}
	rec2 := pipeline.Record{Email: "c@d.com", Name: "C"}

	_ = dlq.Write(rec1, errors.New("err1"))
	_ = dlq.Write(rec2, errors.New("err2"))
	dlq.Close()

	data, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
}
