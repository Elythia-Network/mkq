package mkq

import (
	"context"
	"log/slog"
)

// Logger receives structured records mkq emits at operational
// boundaries (stalled-scan failures, NOSCRIPT reload paths, fetchNext
// shutdown races, BZPopMin pool-size warnings, cron pattern parse
// failures). The default is a noop — unconfigured clients see zero
// behavioural change and no allocation overhead.
//
// Adapters live under github.com/elythia-network/mkq/observability/...:
//
//   - slogadapter: bridges to *slog.Logger.
//
// mkq does not import any of these adapters from its core package;
// callers opt in by importing the adapter sub-package they need.
type Logger interface {
	Debug(msg string, attrs ...slog.Attr)
	Info(msg string, attrs ...slog.Attr)
	Warn(msg string, attrs ...slog.Attr)
	Error(msg string, attrs ...slog.Attr)
}

// Metrics receives counter / histogram / gauge updates from mkq's
// hot paths (Queue.Add, dispatch, handler invocation, finalisation).
// The default is a noop. Names follow Prometheus conventions
// (_total suffix on counters, _seconds suffix on duration histograms);
// adapters that target other systems are free to translate.
//
// All three methods are called with delta semantics — including
// GaugeAdd, which mkq invokes with +1 on dispatch and -1 on finalise
// to track in-flight job count. Adapters must implement GaugeAdd as
// "increment current value by delta", not "set to delta". Backends
// that only expose a Set API can maintain their own running counter.
//
// mkq emits the following signals (initial set):
//
//	mkq_jobs_added_total            counter   queue, name
//	mkq_jobs_processed_total        counter   queue, name, status
//	mkq_handler_duration_seconds    histogram queue, name
//	mkq_dispatch_wait_seconds       histogram queue
//	mkq_jobs_in_flight              gauge     queue (delta-based)
type Metrics interface {
	CounterAdd(name string, delta float64, attrs ...slog.Attr)
	HistogramObserve(name string, value float64, attrs ...slog.Attr)
	GaugeAdd(name string, delta float64, attrs ...slog.Attr)
}

// Tracer creates spans around mkq's user-visible operations.
// Start returns the (possibly-modified) ctx callers should pass to
// any nested operation, plus a Span the caller is responsible for
// ending. The default is a noop.
//
// mkq starts two spans:
//
//	mkq.queue.add        — around Queue.Add
//	mkq.worker.process   — around handler invocation; parent of any
//	                       spans the user creates inside the handler
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...slog.Attr) (context.Context, Span)
}

// Span is the per-operation handle returned by Tracer.Start. End
// finalises the span; SetError marks the span as errored before End.
// All methods are safe to call on a noop span.
type Span interface {
	SetAttrs(attrs ...slog.Attr)
	SetError(err error)
	End()
}

// noopLogger / noopMetrics / noopTracer are returned in place of nil
// so call sites in worker.go / queue.go / cron.go can call methods
// unconditionally without nil checks. Each method body is empty so
// the call is inlinable; the noop branch is the cheap default.
type noopLogger struct{}

func (noopLogger) Debug(string, ...slog.Attr) {}
func (noopLogger) Info(string, ...slog.Attr)  {}
func (noopLogger) Warn(string, ...slog.Attr)  {}
func (noopLogger) Error(string, ...slog.Attr) {}

type noopMetrics struct{}

func (noopMetrics) CounterAdd(string, float64, ...slog.Attr)       {}
func (noopMetrics) HistogramObserve(string, float64, ...slog.Attr) {}
func (noopMetrics) GaugeAdd(string, float64, ...slog.Attr)         {}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...slog.Attr) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) SetAttrs(...slog.Attr) {}
func (noopSpan) SetError(error)        {}
func (noopSpan) End()                  {}

// Metric name constants are exported so adapter authors can register
// the right collector set without copy-pasting strings out of mkq's
// docs. promadapter / oteladapter both rely on these.
const (
	MetricJobsAddedTotal         = "mkq_jobs_added_total"
	MetricJobsProcessedTotal     = "mkq_jobs_processed_total"
	MetricHandlerDurationSeconds = "mkq_handler_duration_seconds"
	MetricDispatchWaitSeconds    = "mkq_dispatch_wait_seconds"
	MetricJobsInFlight           = "mkq_jobs_in_flight"
)

// Span name constants — same rationale as the metric names: adapter
// authors and tracing dashboards reference these by string, and mkq
// owning the canonical spelling avoids drift.
const (
	SpanQueueAdd      = "mkq.queue.add"
	SpanWorkerProcess = "mkq.worker.process"
)

// Attribute key constants used across spans / log records / metric
// labels. Following OTel attribute-name conventions (lowercase,
// dot-separated namespace).
const (
	AttrQueue         = "mkq.queue"
	AttrJobName       = "mkq.job.name"
	AttrJobID         = "mkq.job.id"
	AttrJobAttempts   = "mkq.job.attempts_made"
	AttrProcessStatus = "mkq.process.status" // "completed" | "failed"
	AttrError         = "mkq.error"
)
