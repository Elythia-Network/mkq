// Package promadapter bridges mkq.Metrics to Prometheus collectors.
//
// Usage:
//
//	import "github.com/prometheus/client_golang/prometheus"
//	import "github.com/elythia-network/mkq"
//	import "github.com/elythia-network/mkq/observability/promadapter"
//
//	m := promadapter.New(prometheus.DefaultRegisterer)
//	client, _ := mkq.NewClient(ctx, mkq.Config{
//	    Redis:   redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
//	    Metrics: m,
//	})
//
// The adapter registers five collectors at construction time, one per
// signal mkq emits (see mkq.Metric* constants). Pass a non-default
// registerer if you want to scope mkq's metrics to a sub-registry.
//
// Histogram buckets default to Prometheus's standard set
// (DefBuckets); pass WithBuckets to override per-signal.
package promadapter

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elythia-network/mkq"
)

// Metrics adapts Prometheus collectors to mkq.Metrics. Construct via
// New; do not zero-initialise.
type Metrics struct {
	jobsAdded     *prometheus.CounterVec
	jobsProcessed *prometheus.CounterVec
	handlerDur    *prometheus.HistogramVec
	dispatchWait  *prometheus.HistogramVec
	jobsInFlight  *prometheus.GaugeVec
}

// Option mutates the Metrics during construction. Currently only
// WithBuckets is exposed; reserved for future per-signal customisation.
type Option func(*config)

type config struct {
	handlerBuckets  []float64
	dispatchBuckets []float64
}

// WithHandlerDurationBuckets overrides the histogram buckets for
// mkq_handler_duration_seconds. Default = prometheus.DefBuckets.
func WithHandlerDurationBuckets(b []float64) Option {
	return func(c *config) { c.handlerBuckets = b }
}

// WithDispatchWaitBuckets overrides the histogram buckets for
// mkq_dispatch_wait_seconds. Default = prometheus.DefBuckets.
func WithDispatchWaitBuckets(b []float64) Option {
	return func(c *config) { c.dispatchBuckets = b }
}

// New constructs a Metrics, registers its collectors with reg, and
// returns a ready-to-use adapter. Panics on duplicate registration —
// pass a fresh registerer or a sub-registry to avoid collisions when
// re-instantiating.
func New(reg prometheus.Registerer, opts ...Option) *Metrics {
	cfg := config{
		handlerBuckets:  prometheus.DefBuckets,
		dispatchBuckets: prometheus.DefBuckets,
	}
	for _, o := range opts {
		o(&cfg)
	}
	m := &Metrics{
		jobsAdded: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: mkq.MetricJobsAddedTotal, Help: "Total jobs enqueued via mkq.Queue.Add."},
			[]string{"queue", "name"},
		),
		jobsProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: mkq.MetricJobsProcessedTotal, Help: "Total jobs that reached a terminal state (completed or failed)."},
			[]string{"queue", "name", "status"},
		),
		handlerDur: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    mkq.MetricHandlerDurationSeconds,
				Help:    "Handler invocation duration in seconds.",
				Buckets: cfg.handlerBuckets,
			},
			[]string{"queue", "name"},
		),
		dispatchWait: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    mkq.MetricDispatchWaitSeconds,
				Help:    "Time from job enqueue to handler start, in seconds.",
				Buckets: cfg.dispatchBuckets,
			},
			[]string{"queue"},
		),
		jobsInFlight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: mkq.MetricJobsInFlight, Help: "Jobs currently held by a worker (between dispatch and finalise)."},
			[]string{"queue"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.jobsAdded, m.jobsProcessed, m.handlerDur, m.dispatchWait, m.jobsInFlight)
	}
	return m
}

// CounterAdd dispatches mkq's counter signals to the right CounterVec.
// Unknown counter names are dropped silently — adapter callers should
// treat the metric set as the authoritative list and not invent
// custom names.
func (m *Metrics) CounterAdd(name string, delta float64, attrs ...slog.Attr) {
	labels := attrsToLabels(attrs)
	switch name {
	case mkq.MetricJobsAddedTotal:
		m.jobsAdded.With(prometheus.Labels{
			"queue": labels[mkq.AttrQueue],
			"name":  labels[mkq.AttrJobName],
		}).Add(delta)
	case mkq.MetricJobsProcessedTotal:
		m.jobsProcessed.With(prometheus.Labels{
			"queue":  labels[mkq.AttrQueue],
			"name":   labels[mkq.AttrJobName],
			"status": labels[mkq.AttrProcessStatus],
		}).Add(delta)
	}
}

// HistogramObserve dispatches mkq's histogram signals.
func (m *Metrics) HistogramObserve(name string, value float64, attrs ...slog.Attr) {
	labels := attrsToLabels(attrs)
	switch name {
	case mkq.MetricHandlerDurationSeconds:
		m.handlerDur.With(prometheus.Labels{
			"queue": labels[mkq.AttrQueue],
			"name":  labels[mkq.AttrJobName],
		}).Observe(value)
	case mkq.MetricDispatchWaitSeconds:
		m.dispatchWait.With(prometheus.Labels{
			"queue": labels[mkq.AttrQueue],
		}).Observe(value)
	}
}

// GaugeAdd applies the delta to the matching GaugeVec. mkq emits +1
// on dispatch and -1 on finalise so the gauge tracks the running
// in-flight count.
func (m *Metrics) GaugeAdd(name string, delta float64, attrs ...slog.Attr) {
	labels := attrsToLabels(attrs)
	switch name {
	case mkq.MetricJobsInFlight:
		m.jobsInFlight.With(prometheus.Labels{
			"queue": labels[mkq.AttrQueue],
		}).Add(delta)
	}
}

// attrsToLabels flattens slog.Attr slices into a string map. mkq only
// passes string-valued attrs to the metrics interface; non-string
// values are stringified via slog's standard Resolve path.
func attrsToLabels(attrs []slog.Attr) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, a := range attrs {
		out[a.Key] = a.Value.Resolve().String()
	}
	return out
}
