package mkq_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// fakeLogger captures each call so tests can assert level + msg + attrs.
type fakeLogger struct {
	mu      sync.Mutex
	records []logRecord
}

type logRecord struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

func (f *fakeLogger) log(lvl slog.Level, msg string, attrs []slog.Attr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, logRecord{level: lvl, msg: msg, attrs: append([]slog.Attr(nil), attrs...)})
}
func (f *fakeLogger) Debug(msg string, attrs ...slog.Attr) { f.log(slog.LevelDebug, msg, attrs) }
func (f *fakeLogger) Info(msg string, attrs ...slog.Attr)  { f.log(slog.LevelInfo, msg, attrs) }
func (f *fakeLogger) Warn(msg string, attrs ...slog.Attr)  { f.log(slog.LevelWarn, msg, attrs) }
func (f *fakeLogger) Error(msg string, attrs ...slog.Attr) { f.log(slog.LevelError, msg, attrs) }

// fakeMetrics captures counter / histogram / gauge calls.
type fakeMetrics struct {
	mu             sync.Mutex
	counters       map[string]float64
	counterAttrs   map[string][]slog.Attr
	histograms     map[string][]float64
	histogramAttrs map[string][]slog.Attr
	gauges         map[string]float64
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		counters:       map[string]float64{},
		counterAttrs:   map[string][]slog.Attr{},
		histograms:     map[string][]float64{},
		histogramAttrs: map[string][]slog.Attr{},
		gauges:         map[string]float64{},
	}
}

func metricsKey(name string, attrs []slog.Attr) string {
	k := name
	for _, a := range attrs {
		k += "|" + a.Key + "=" + a.Value.String()
	}
	return k
}

func (f *fakeMetrics) CounterAdd(name string, delta float64, attrs ...slog.Attr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := metricsKey(name, attrs)
	f.counters[k] += delta
	f.counterAttrs[k] = append([]slog.Attr(nil), attrs...)
}
func (f *fakeMetrics) HistogramObserve(name string, value float64, attrs ...slog.Attr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := metricsKey(name, attrs)
	f.histograms[k] = append(f.histograms[k], value)
	f.histogramAttrs[k] = append([]slog.Attr(nil), attrs...)
}
func (f *fakeMetrics) GaugeAdd(name string, delta float64, attrs ...slog.Attr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := metricsKey(name, attrs)
	f.gauges[k] += delta
}

func (f *fakeMetrics) counter(name string, attrs ...slog.Attr) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counters[metricsKey(name, attrs)]
}

func (f *fakeMetrics) histogramCount(name string, attrs ...slog.Attr) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.histograms[metricsKey(name, attrs)])
}

func (f *fakeMetrics) gauge(name string, attrs ...slog.Attr) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gauges[metricsKey(name, attrs)]
}

// fakeTracer / fakeSpan record span shape for assertions.
type fakeTracer struct {
	mu    sync.Mutex
	spans []*fakeSpan
}

type fakeSpan struct {
	mu     sync.Mutex
	name   string
	attrs  []slog.Attr
	err    error
	ended  bool
	parent *fakeSpan
}

type tracerCtxKey struct{}

func (t *fakeTracer) Start(ctx context.Context, name string, attrs ...slog.Attr) (context.Context, mkq.Span) {
	parent, _ := ctx.Value(tracerCtxKey{}).(*fakeSpan)
	sp := &fakeSpan{name: name, attrs: append([]slog.Attr(nil), attrs...), parent: parent}
	t.mu.Lock()
	t.spans = append(t.spans, sp)
	t.mu.Unlock()
	return context.WithValue(ctx, tracerCtxKey{}, sp), sp
}

func (t *fakeTracer) snapshot() []*fakeSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*fakeSpan(nil), t.spans...)
}

func (s *fakeSpan) SetAttrs(attrs ...slog.Attr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attrs = append(s.attrs, attrs...)
}
func (s *fakeSpan) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}
func (s *fakeSpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
}

func (s *fakeSpan) attr(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.attrs {
		if a.Key == key {
			return a.Value.String(), true
		}
	}
	return "", false
}

// newClientWithObservers wraps newClient but forces in our fake
// observers. Returns the fakes alongside the client.
func newClientWithObservers(t *testing.T, prefix string) (*mkq.Client, *fakeLogger, *fakeMetrics, *fakeTracer) {
	t.Helper()
	logger := &fakeLogger{}
	metrics := newFakeMetrics()
	tracer := &fakeTracer{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{testRedisAddr()}},
		KeyPrefix: prefix,
		Logger:    logger,
		Metrics:   metrics,
		Tracer:    tracer,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		flushPrefix(t, prefix)
		_ = c.Close()
	})
	return c, logger, metrics, tracer
}

// TestObservability_NoopDefaults guards the zero-config path: a Client
// constructed with no Logger / Metrics / Tracer must Add and Process
// jobs without panicking on the noop dispatches inside the hot path.
func TestObservability_NoopDefaults(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix) // no observers
	queue := mkq.Define[testPayload](c, "noop")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Body: "x"})
	require.NoError(t, err)

	processed := make(chan struct{}, 1)
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		processed <- struct{}{}
		return nil, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked under noop observers")
	}
}

// TestObservability_QueueAdd_RecordsCounterAndSpan asserts Queue.Add
// fires mkq_jobs_added_total and opens an mkq.queue.add span with the
// expected attribute set.
func TestObservability_QueueAdd_RecordsCounterAndSpan(t *testing.T) {
	prefix := uniquePrefix(t)
	c, _, metrics, tracer := newClientWithObservers(t, prefix)
	queue := mkq.Define[testPayload](c, "addmetric")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Body: "x"})
	require.NoError(t, err)

	got := metrics.counter(mkq.MetricJobsAddedTotal,
		slog.String(mkq.AttrQueue, "addmetric"),
		slog.String(mkq.AttrJobName, "addmetric"),
	)
	assert.EqualValues(t, 1, got, "jobs_added_total must increment once")

	spans := tracer.snapshot()
	require.NotEmpty(t, spans)
	addSpan := spans[0]
	assert.Equal(t, mkq.SpanQueueAdd, addSpan.name)
	if v, ok := addSpan.attr(mkq.AttrJobID); !ok || v != job.ID {
		t.Errorf("queue.add span should carry job.id=%q, got %q (ok=%v)", job.ID, v, ok)
	}
	assert.True(t, addSpan.ended, "queue.add span must end before Add returns")
}

// TestObservability_Worker_RecordsLifecycle asserts a successful
// handler run records dispatch_wait + handler_duration histograms,
// jobs_processed_total{status=completed}, and the
// mkq.worker.process span. jobs_in_flight must net to zero after
// the run.
func TestObservability_Worker_RecordsLifecycle(t *testing.T) {
	prefix := uniquePrefix(t)
	c, _, metrics, tracer := newClientWithObservers(t, prefix)
	queue := mkq.Define[testPayload](c, "lifecycle")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Body: "ok"})
	require.NoError(t, err)

	processed := make(chan struct{}, 1)
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		processed <- struct{}{}
		return "ret", nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("handler timeout")
	}

	// finalise + counter increment is async to the handler return.
	assert.Eventually(t, func() bool {
		return metrics.counter(mkq.MetricJobsProcessedTotal,
			slog.String(mkq.AttrQueue, "lifecycle"),
			slog.String(mkq.AttrJobName, "lifecycle"),
			slog.String(mkq.AttrProcessStatus, "completed"),
		) == 1
	}, 3*time.Second, 50*time.Millisecond, "jobs_processed_total{status=completed} must increment once")

	assert.GreaterOrEqual(t, metrics.histogramCount(mkq.MetricHandlerDurationSeconds,
		slog.String(mkq.AttrQueue, "lifecycle"),
		slog.String(mkq.AttrJobName, "lifecycle"),
	), 1, "handler_duration_seconds must observe at least once")

	assert.GreaterOrEqual(t, metrics.histogramCount(mkq.MetricDispatchWaitSeconds,
		slog.String(mkq.AttrQueue, "lifecycle"),
	), 1, "dispatch_wait_seconds must observe at least once")

	assert.Eventually(t, func() bool {
		return metrics.gauge(mkq.MetricJobsInFlight, slog.String(mkq.AttrQueue, "lifecycle")) == 0
	}, 2*time.Second, 50*time.Millisecond, "jobs_in_flight must net to zero after handler")

	// span shape
	var processSpan *fakeSpan
	for _, s := range tracer.snapshot() {
		if s.name == mkq.SpanWorkerProcess {
			processSpan = s
			break
		}
	}
	require.NotNil(t, processSpan, "mkq.worker.process span must be created")
	assert.True(t, processSpan.ended)
	if v, ok := processSpan.attr(mkq.AttrQueue); !ok || v != "lifecycle" {
		t.Errorf("process span queue attr = %q (ok=%v), want lifecycle", v, ok)
	}
}

// TestObservability_Worker_FailedTerminalRecordsFailedStatus runs a
// handler that always errors with attempts=1, so the first failure
// is the terminal one. Asserts the failed-status counter increments
// and span error is recorded.
func TestObservability_Worker_FailedTerminalRecordsFailedStatus(t *testing.T) {
	prefix := uniquePrefix(t)
	c, _, metrics, tracer := newClientWithObservers(t, prefix)
	queue := mkq.Define[testPayload](c, "fail")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Body: "boom"}, mkq.WithAttempts(1))
	require.NoError(t, err)

	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("intentional failure")
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	assert.Eventually(t, func() bool {
		return metrics.counter(mkq.MetricJobsProcessedTotal,
			slog.String(mkq.AttrQueue, "fail"),
			slog.String(mkq.AttrJobName, "fail"),
			slog.String(mkq.AttrProcessStatus, "failed"),
		) == 1
	}, 3*time.Second, 50*time.Millisecond, "failed status counter must fire on terminal failure")

	// span error must be recorded
	var processSpan *fakeSpan
	for _, s := range tracer.snapshot() {
		if s.name == mkq.SpanWorkerProcess {
			processSpan = s
			break
		}
	}
	require.NotNil(t, processSpan)
	assert.NotNil(t, processSpan.err, "process span must record handler error on terminal failure")
}

// TestObservability_Worker_RetryDoesNotCountAsTerminal ensures a
// retry-bound failure (attempts>1, first attempt) does NOT bump
// jobs_processed_total — only the eventual terminal state counts.
func TestObservability_Worker_RetryDoesNotCountAsTerminal(t *testing.T) {
	prefix := uniquePrefix(t)
	c, _, metrics, _ := newClientWithObservers(t, prefix)
	queue := mkq.Define[testPayload](c, "retry")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{}, mkq.WithAttempts(3))
	require.NoError(t, err)

	var attempts int
	var mu sync.Mutex
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n == 1 {
			return nil, fmt.Errorf("retry once")
		}
		return nil, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	assert.Eventually(t, func() bool {
		return metrics.counter(mkq.MetricJobsProcessedTotal,
			slog.String(mkq.AttrQueue, "retry"),
			slog.String(mkq.AttrJobName, "retry"),
			slog.String(mkq.AttrProcessStatus, "completed"),
		) == 1
	}, 5*time.Second, 50*time.Millisecond, "second-attempt success must increment completed counter")

	// failed counter must stay at zero — first-attempt failure was a retry, not terminal.
	failedCount := metrics.counter(mkq.MetricJobsProcessedTotal,
		slog.String(mkq.AttrQueue, "retry"),
		slog.String(mkq.AttrJobName, "retry"),
		slog.String(mkq.AttrProcessStatus, "failed"),
	)
	assert.EqualValues(t, 0, failedCount, "retry-bound first attempt must not increment failed counter")
}
