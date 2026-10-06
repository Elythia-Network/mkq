package oteladapter

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/elythia-network/mkq"
)

// Compile-time assertion: *Tracer must satisfy mkq.Tracer.
var _ mkq.Tracer = (*Tracer)(nil)
var _ mkq.Span = (*Span)(nil)

// newTestTracer wires an in-memory exporter so tests can inspect spans.
func newTestTracer(t *testing.T) (*Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	return New(tp.Tracer("mkq-test")), exp
}

// TestTracer_Start_RecordsAttributes verifies span name and the
// initial attribute set propagate to the exporter.
func TestTracer_Start_RecordsAttributes(t *testing.T) {
	tr, exp := newTestTracer(t)

	_, span := tr.Start(context.Background(), mkq.SpanQueueAdd,
		slog.String(mkq.AttrQueue, "q1"),
		slog.String(mkq.AttrJobName, "n1"),
		slog.Int("retries", 3),
	)
	span.End()

	got := exp.GetSpans()
	if len(got) != 1 {
		t.Fatalf("expected 1 span, got %d", len(got))
	}
	s := got[0]
	if s.Name != mkq.SpanQueueAdd {
		t.Errorf("span name = %q, want %q", s.Name, mkq.SpanQueueAdd)
	}
	want := map[string]attribute.Value{
		mkq.AttrQueue:   attribute.StringValue("q1"),
		mkq.AttrJobName: attribute.StringValue("n1"),
		"retries":       attribute.Int64Value(3),
	}
	for _, kv := range s.Attributes {
		if w, ok := want[string(kv.Key)]; ok {
			if kv.Value != w {
				t.Errorf("attr %q = %v, want %v", kv.Key, kv.Value.String(), w.String())
			}
			delete(want, string(kv.Key))
		}
	}
	for k := range want {
		t.Errorf("missing attribute: %q", k)
	}
}

// TestSpan_SetAttrs_Appends ensures SetAttrs after Start adds to the
// same span (visible in the exported span's Attributes slice).
func TestSpan_SetAttrs_Appends(t *testing.T) {
	tr, exp := newTestTracer(t)

	_, span := tr.Start(context.Background(), mkq.SpanWorkerProcess,
		slog.String(mkq.AttrQueue, "q1"))
	span.SetAttrs(slog.String(mkq.AttrJobID, "id-99"))
	span.End()

	got := exp.GetSpans()
	if len(got) != 1 {
		t.Fatalf("expected 1 span")
	}
	keys := map[string]bool{}
	for _, kv := range got[0].Attributes {
		keys[string(kv.Key)] = true
	}
	if !keys[mkq.AttrQueue] || !keys[mkq.AttrJobID] {
		t.Errorf("missing attrs after SetAttrs: keys=%v", keys)
	}
}

// TestSpan_SetError_MarksError verifies SetError flips the span status
// to Error and records the error event.
func TestSpan_SetError_MarksError(t *testing.T) {
	tr, exp := newTestTracer(t)
	_, span := tr.Start(context.Background(), "test")
	span.SetError(errors.New("boom"))
	span.End()

	got := exp.GetSpans()
	if len(got) != 1 {
		t.Fatalf("expected 1 span")
	}
	if got[0].Status.Code != codes.Error {
		t.Errorf("status code = %v, want Error", got[0].Status.Code)
	}
	if got[0].Status.Description != "boom" {
		t.Errorf("status desc = %q, want \"boom\"", got[0].Status.Description)
	}
	hasErrEvent := false
	for _, ev := range got[0].Events {
		if ev.Name == "exception" {
			hasErrEvent = true
		}
	}
	if !hasErrEvent {
		t.Errorf("expected exception event, got events: %v", got[0].Events)
	}
}

// TestSpan_SetError_NilNoOp guards against panics on nil error
// (callers may pass `err` unconditionally in deferred cleanup).
func TestSpan_SetError_NilNoOp(t *testing.T) {
	tr, exp := newTestTracer(t)
	_, span := tr.Start(context.Background(), "test")
	span.SetError(nil)
	span.End()

	got := exp.GetSpans()
	if len(got) != 1 {
		t.Fatalf("expected 1 span")
	}
	if got[0].Status.Code == codes.Error {
		t.Errorf("nil SetError should not flip status to Error")
	}
}

// TestTracer_ChildSpansNest verifies that a span created on a ctx
// already carrying a parent span becomes its child — proves
// Tracer.Start propagates ctx correctly so handler-side user spans
// nest under mkq.worker.process.
func TestTracer_ChildSpansNest(t *testing.T) {
	tr, exp := newTestTracer(t)
	parentCtx, parent := tr.Start(context.Background(), "parent")
	_, child := tr.Start(parentCtx, "child")
	child.End()
	parent.End()

	got := exp.GetSpans()
	if len(got) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(got))
	}
	// Children are exported before parents (sync exporter, FIFO of End calls).
	c, p := got[0], got[1]
	if c.Name != "child" || p.Name != "parent" {
		t.Fatalf("unexpected order: [%q, %q]", c.Name, p.Name)
	}
	if c.Parent.SpanID() != p.SpanContext.SpanID() {
		t.Errorf("child not parented to parent span: child.parent=%v, parent.id=%v",
			c.Parent.SpanID(), p.SpanContext.SpanID())
	}
}
