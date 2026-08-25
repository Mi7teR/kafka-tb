package obs

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegisterAndCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.RecordsTotal.WithLabelValues("ok").Add(2)
	m.DLQTotal.WithLabelValues("poison", "decode").Inc()

	require.Equal(t, 2.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("poison", "decode")))
}

// M4: registering the same metric names twice against the same registry
// must panic (promauto's documented behaviour on a duplicate registration) —
// this pins that NewMetrics does not swallow or dedupe that failure.
func TestNewMetricsTwiceOnSameRegistryPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	require.Panics(t, func() { NewMetrics(reg) })
}

// TestNilMetricsIsNoOp verifies every helper tolerates a nil *Metrics, which
// is what sink and batcher constructors receive in tests that do not build a
// registry.
func TestNilMetricsIsNoOp(t *testing.T) {
	var m *Metrics
	require.NotPanics(t, func() {
		m.IncRecords("ok")
		m.IncEvents("ok")
		m.IncDLQ("poison", "decode")
		m.ObserveBatchSize(10)
		m.ObserveTBLatency("create_transfers", 0)
		m.SetCommitLag("payments", 0, 1)
	})
}

// The helper methods, not the raw vectors, are what the sink and the batcher
// call. Going through them pins both the value and the label set an operator's
// alert selects on: a counter incremented under the wrong label name still
// increments, and nothing but a test like this notices.
func TestHelpersWriteExpectedNamesAndLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.IncRecords("ok")
	m.IncRecords("ok")
	m.IncRecords("poison")
	m.IncEvents("rejected")
	m.IncDLQ("poison", "decode")
	m.SetCommitLag("payments", 3, 17)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP kafkatb_records_total Kafka records processed, by result (ok|rejected|poison|blocked). One per record.
# TYPE kafkatb_records_total counter
kafkatb_records_total{result="ok"} 2
kafkatb_records_total{result="poison"} 1
`), "kafkatb_records_total"))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP kafkatb_events_total Events applied to TigerBeetle, by result (ok|rejected). One per event, so a record carrying many transfers contributes many.
# TYPE kafkatb_events_total counter
kafkatb_events_total{result="rejected"} 1
`), "kafkatb_events_total"))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP kafkatb_dlq_total Records written to the dead-letter topic, by reason and error.
# TYPE kafkatb_dlq_total counter
kafkatb_dlq_total{error="decode",reason="poison"} 1
`), "kafkatb_dlq_total"))

	// The partition is an int32 on the wire and a label value here; it must
	// render as a decimal number, not as Go's default rendering of a number.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP kafkatb_offset_commit_lag Offsets between the highest tracked record and the committed watermark, by topic/partition.
# TYPE kafkatb_offset_commit_lag gauge
kafkatb_offset_commit_lag{partition="3",topic="payments"} 17
`), "kafkatb_offset_commit_lag"))
}

// Records and events are separate counters on purpose: one record carrying
// many transfers is one record and many events. Incrementing one must not
// move the other.
func TestRecordsAndEventsAreSeparateCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.IncRecords("ok")
	m.IncEvents("ok")
	m.IncEvents("ok")
	m.IncEvents("ok")

	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")))
	require.Equal(t, 3.0, testutil.ToFloat64(m.EventsTotal.WithLabelValues("ok")))
}

// bucketsOf returns the upper bounds of the single histogram in family name,
// for the series carrying labels.
func bucketsOf(t *testing.T, reg *prometheus.Registry, name string) []float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		require.Len(t, f.GetMetric(), 1, "want exactly one series in %s", name)
		var out []float64
		for _, b := range f.GetMetric()[0].GetHistogram().GetBucket() {
			out = append(out, b.GetUpperBound())
		}
		return out
	}
	t.Fatalf("no metric family %q", name)
	return nil
}

// The batch-size buckets are chosen against TigerBeetle's 8189-event limit:
// the top bucket is that limit, so "batches at the cap" is a bucket an
// operator can actually select. Changing these silently rewrites every
// existing histogram_quantile.
func TestBatchSizeBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.ObserveBatchSize(1500)

	require.Equal(t,
		[]float64{1, 10, 100, 500, 1000, 2000, 4000, 8189},
		bucketsOf(t, reg, "kafkatb_tb_batch_size"))

	require.Equal(t, 1, testutil.CollectAndCount(m.BatchSize),
		"one histogram, not one per call")
}

// A 1500-event batch belongs above 1000 and at or below 2000. This is what
// makes the bucket list above meaningful rather than a copy of the source.
func TestObserveBatchSizeLandsInTheRightBucket(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.ObserveBatchSize(1500)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != "kafkatb_tb_batch_size" {
			continue
		}
		h := f.GetMetric()[0].GetHistogram()
		require.Equal(t, uint64(1), h.GetSampleCount())
		require.Equal(t, 1500.0, h.GetSampleSum())
		for _, b := range h.GetBucket() {
			want := uint64(0)
			if b.GetUpperBound() >= 1500 {
				want = 1
			}
			require.Equal(t, want, b.GetCumulativeCount(),
				"cumulative count at le=%v", b.GetUpperBound())
		}
		return
	}
	t.Fatal("no kafkatb_tb_batch_size family")
}

// The latency buckets span 1ms to ~8.2s exponentially: a TigerBeetle call
// that has gone from milliseconds to seconds has to remain visible rather
// than pile into +Inf.
func TestTBLatencyBucketsAndLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.ObserveTBLatency("create_transfers", 1500*time.Millisecond)

	want := make([]float64, 0, 14)
	for i, v := 0, 0.001; i < 14; i, v = i+1, v*2 {
		want = append(want, v)
	}
	got := bucketsOf(t, reg, "kafkatb_tb_latency_seconds")
	// The length is asserted separately and first: InDeltaSlice iterates over
	// the actual slice, so a truncated bucket list would otherwise pass.
	require.Len(t, got, 14)
	require.InDeltaSlice(t, want, got, 1e-9)
	require.InDelta(t, 8.192, got[len(got)-1], 1e-9,
		"the top bucket has to stay above a seconds-scale TigerBeetle call")

	// The duration is observed in seconds, not nanoseconds: a Duration passed
	// straight through would land 1.5e9 in a histogram that tops out at 8.192.
	families, err := reg.Gather()
	require.NoError(t, err)
	var found bool
	for _, f := range families {
		if f.GetName() != "kafkatb_tb_latency_seconds" {
			continue
		}
		found = true
		series := f.GetMetric()[0]
		require.Equal(t, []string{"op"}, labelNames(series),
			"an alert selecting by op depends on this label name")
		require.Equal(t, "create_transfers", series.GetLabel()[0].GetValue())
		require.InDelta(t, 1.5, series.GetHistogram().GetSampleSum(), 1e-9)
		require.Equal(t, uint64(1), series.GetHistogram().GetSampleCount())
	}
	require.True(t, found, "no kafkatb_tb_latency_seconds family")
}

// labelNames returns the label names of one series, in exposition order.
func labelNames(m *dto.Metric) []string {
	var out []string
	for _, l := range m.GetLabel() {
		out = append(out, l.GetName())
	}
	return out
}
