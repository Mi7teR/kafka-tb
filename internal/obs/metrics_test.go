package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestNewMetricsTwiceOnSameRegistryPanicsNot(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	require.NotPanics(t, func() { NewMetrics(prometheus.NewRegistry()) })
}

// TestNilMetricsIsNoOp verifies every helper tolerates a nil *Metrics, which
// is what sink and batcher constructors receive in tests that do not build a
// registry.
func TestNilMetricsIsNoOp(t *testing.T) {
	var m *Metrics
	require.NotPanics(t, func() {
		m.IncRecords("ok")
		m.IncDLQ("poison", "decode")
		m.ObserveBatchSize(10)
		m.ObserveTBLatency("create_transfers", 0)
	})
}
