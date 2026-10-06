package promadapter

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/elythia-network/mkq"
)

// Compile-time assertion: *Metrics must satisfy mkq.Metrics.
var _ mkq.Metrics = (*Metrics)(nil)

// TestMetrics_CounterPaths ensures each known counter name routes to
// the right CounterVec with the right labels, and unknown names are
// dropped without panic.
func TestMetrics_CounterPaths(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.CounterAdd(mkq.MetricJobsAddedTotal, 1,
		slog.String(mkq.AttrQueue, "q1"),
		slog.String(mkq.AttrJobName, "n1"),
	)
	m.CounterAdd(mkq.MetricJobsProcessedTotal, 1,
		slog.String(mkq.AttrQueue, "q1"),
		slog.String(mkq.AttrJobName, "n1"),
		slog.String(mkq.AttrProcessStatus, "completed"),
	)
	m.CounterAdd("mkq_unknown_counter", 99) // must not panic / register

	if got := testutil.ToFloat64(m.jobsAdded.WithLabelValues("q1", "n1")); got != 1 {
		t.Errorf("jobsAdded = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.jobsProcessed.WithLabelValues("q1", "n1", "completed")); got != 1 {
		t.Errorf("jobsProcessed = %v, want 1", got)
	}
}

// TestMetrics_HistogramPaths verifies the histogram routing.
func TestMetrics_HistogramPaths(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.HistogramObserve(mkq.MetricHandlerDurationSeconds, 0.123,
		slog.String(mkq.AttrQueue, "q1"),
		slog.String(mkq.AttrJobName, "n1"),
	)
	m.HistogramObserve(mkq.MetricDispatchWaitSeconds, 0.5,
		slog.String(mkq.AttrQueue, "q1"),
	)

	if got := testutil.CollectAndCount(m.handlerDur); got != 1 {
		t.Errorf("handlerDur series count = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.dispatchWait); got != 1 {
		t.Errorf("dispatchWait series count = %v, want 1", got)
	}
}

// TestMetrics_GaugeJobsInFlight verifies +1/-1 paired calls leave the
// gauge at zero — mirrors how mkq's runJob brackets the handler.
func TestMetrics_GaugeJobsInFlight(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	q := slog.String(mkq.AttrQueue, "q1")
	m.GaugeAdd(mkq.MetricJobsInFlight, 1, q)
	m.GaugeAdd(mkq.MetricJobsInFlight, 1, q)
	m.GaugeAdd(mkq.MetricJobsInFlight, -1, q)

	if got := testutil.ToFloat64(m.jobsInFlight.WithLabelValues("q1")); got != 1 {
		t.Errorf("inFlight = %v, want 1", got)
	}

	m.GaugeAdd(mkq.MetricJobsInFlight, -1, q)
	if got := testutil.ToFloat64(m.jobsInFlight.WithLabelValues("q1")); got != 0 {
		t.Errorf("inFlight after pair = %v, want 0", got)
	}
}

// TestMetrics_RegistersAllSignals confirms a fresh Registry gathers
// the full mkq metric set after observation. Prometheus elides
// no-series metric families from Gather, so the test seeds one
// observation per signal before gathering.
func TestMetrics_RegistersAllSignals(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	q := slog.String(mkq.AttrQueue, "q1")
	n := slog.String(mkq.AttrJobName, "n1")
	m.CounterAdd(mkq.MetricJobsAddedTotal, 1, q, n)
	m.CounterAdd(mkq.MetricJobsProcessedTotal, 1, q, n,
		slog.String(mkq.AttrProcessStatus, "completed"))
	m.HistogramObserve(mkq.MetricHandlerDurationSeconds, 0.1, q, n)
	m.HistogramObserve(mkq.MetricDispatchWaitSeconds, 0.1, q)
	m.GaugeAdd(mkq.MetricJobsInFlight, 1, q)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		mkq.MetricJobsAddedTotal:         false,
		mkq.MetricJobsProcessedTotal:     false,
		mkq.MetricHandlerDurationSeconds: false,
		mkq.MetricDispatchWaitSeconds:    false,
		mkq.MetricJobsInFlight:           false,
	}
	for _, mf := range mfs {
		if _, ok := want[*mf.Name]; ok {
			want[*mf.Name] = true
		}
	}
	missing := []string{}
	for n, seen := range want {
		if !seen {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		t.Errorf("missing registered metrics: %s", strings.Join(missing, ", "))
	}
}
