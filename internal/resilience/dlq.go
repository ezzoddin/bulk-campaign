package resilience

import (
	"bulk-campaign/internal/pipeline"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// FailedRecord captures a record that could not be processed after retries.
type FailedRecord struct {
	Record    pipeline.Record `json:"record"`
	Error     string          `json:"error"`
	Timestamp time.Time       `json:"timestamp"`
}

// DLQ writes permanently failed records to disk in JSONL format.
type DLQ struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

// NewDLQ opens (or creates) the DLQ file at path.
func NewDLQ(path string) (*DLQ, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("dlq: open %s: %w", path, err)
	}
	return &DLQ{path: path, f: f}, nil
}

// Write appends a failed record to the DLQ file.
func (d *DLQ) Write(rec pipeline.Record, err error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	fr := FailedRecord{
		Record:    rec,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	}

	line, jsonErr := json.Marshal(fr)
	if jsonErr != nil {
		return fmt.Errorf("dlq: marshal: %w", jsonErr)
	}

	if _, writeErr := d.f.Write(append(line, '\n')); writeErr != nil {
		return fmt.Errorf("dlq: write: %w", writeErr)
	}

	return nil
}

// Close flushes and closes the DLQ file.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.f.Close()
}
