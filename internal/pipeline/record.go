package pipeline

// Record represents a single row from the CSV input.
// Fields are kept as strings here; validation/transformation
// happens downstream in the notifier layer.
type Record struct {
	Email   string
	Phone   string
	Name    string
	Payload map[string]string // extra columns, keyed by header name
}
