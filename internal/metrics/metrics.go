package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RecordsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bulk_campaign_records_processed_total",
			Help: "Total number of records processed",
		},
		[]string{"status"}, // "success" | "failed"
	)

	NotificationsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bulk_campaign_notifications_sent_total",
			Help: "Total number of notifications sent",
		},
		[]string{"channel"}, // "email" | "sms" | "telegram"
	)

	UploadDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "bulk_campaign_upload_duration_seconds",
			Help:    "Time taken to process an uploaded CSV",
			Buckets: prometheus.DefBuckets,
		},
	)
)
