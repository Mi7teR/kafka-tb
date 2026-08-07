// Package obs provides the connector's Prometheus instrumentation and its
// HTTP health/metrics endpoints.
package obs

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the connector's Prometheus instrumentation. A nil *Metrics
// is a valid receiver for every method below: callers that were built
// without a registry (most unit tests) pass nil and every Inc/Observe here
// becomes a no-op instead of a nil-pointer dereference.
type Metrics struct {
	RecordsTotal *prometheus.CounterVec
	DLQTotal     *prometheus.CounterVec
	BatchSize    prometheus.Histogram
	TBLatency    *prometheus.HistogramVec
	CommitLag    prometheus.Gauge
}

// NewMetrics registers the connector's metrics against reg and returns them.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		RecordsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkatb_records_total",
			Help: "Kafka records processed, by result (ok|rejected|poison|blocked).",
		}, []string{"result"}),
		DLQTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkatb_dlq_total",
			Help: "Records written to the dead-letter topic, by reason and error.",
		}, []string{"reason", "error"}),
		BatchSize: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafkatb_tb_batch_size",
			Help:    "Size of batches sent to TigerBeetle.",
			Buckets: []float64{1, 10, 100, 500, 1000, 2000, 4000, 8189},
		}),
		TBLatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafkatb_tb_latency_seconds",
			Help:    "Latency of TigerBeetle calls, by operation.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"op"}),
		CommitLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "kafkatb_offset_commit_lag",
			Help: "Offset commit lag.",
		}),
	}
}

// IncRecords increments RecordsTotal for result. No-op on a nil *Metrics.
func (m *Metrics) IncRecords(result string) {
	if m == nil {
		return
	}
	m.RecordsTotal.WithLabelValues(result).Inc()
}

// IncDLQ increments DLQTotal for reason/error. No-op on a nil *Metrics.
func (m *Metrics) IncDLQ(reason, errName string) {
	if m == nil {
		return
	}
	m.DLQTotal.WithLabelValues(reason, errName).Inc()
}

// ObserveBatchSize records the size of a batch sent to TigerBeetle. No-op on
// a nil *Metrics.
func (m *Metrics) ObserveBatchSize(n int) {
	if m == nil {
		return
	}
	m.BatchSize.Observe(float64(n))
}

// ObserveTBLatency records the duration of a TigerBeetle call for op. No-op
// on a nil *Metrics.
func (m *Metrics) ObserveTBLatency(op string, d time.Duration) {
	if m == nil {
		return
	}
	m.TBLatency.WithLabelValues(op).Observe(d.Seconds())
}
