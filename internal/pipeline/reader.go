package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
)

// Reader streams CSV rows into a channel one record at a time.
// It never loads the full file into memory — safe for millions of rows.
//
// The caller is responsible for closing src (e.g. *os.File).
// The returned channel is closed when EOF or ctx is cancelled.
func Reader(ctx context.Context, src io.Reader) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errc := make(chan error, 1) // buffered: writer never blocks on send

	go func() {
		defer close(out)
		defer close(errc)

		r := csv.NewReader(src)
		r.TrimLeadingSpace = true
		r.ReuseRecord = false // safe for concurrent consumers

		// First row is the header
		headers, err := r.Read()
		if err != nil {
			errc <- fmt.Errorf("reading header: %w", err)
			return
		}

		// Build a column-index map for O(1) lookups
		idx := make(map[string]int, len(headers))
		for i, h := range headers {
			idx[h] = i
		}

		for {
			// Honour cancellation between rows
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			default:
			}

			row, err := r.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				errc <- fmt.Errorf("reading row: %w", err)
				return
			}

			rec := recordFromRow(row, idx)

			select {
			case out <- rec:
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			}
		}
	}()

	return out, errc
}

// recordFromRow maps a raw CSV row into a Record using the header index.
func recordFromRow(row []string, idx map[string]int) Record {
	get := func(key string) string {
		if i, ok := idx[key]; ok && i < len(row) {
			return row[i]
		}
		return ""
	}

	rec := Record{
		Email:   get("email"),
		Phone:   get("phone"),
		Name:    get("name"),
		Payload: make(map[string]string),
	}

	// Capture any extra columns into Payload
	known := map[string]bool{"email": true, "phone": true, "name": true}
	for col, i := range idx {
		if !known[col] && i < len(row) {
			rec.Payload[col] = row[i]
		}
	}

	return rec
}
