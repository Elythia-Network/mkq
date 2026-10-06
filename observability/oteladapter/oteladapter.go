// Package oteladapter bridges mkq.Tracer to OpenTelemetry trace.Tracer.
//
// Usage:
//
//	import "go.opentelemetry.io/otel"
//	import "github.com/elythia-network/mkq"
//	import "github.com/elythia-network/mkq/observability/oteladapter"
//
//	tracer := otel.Tracer("mkq")
//	client, _ := mkq.NewClient(ctx, mkq.Config{
//	    Redis:  redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
//	    Tracer: oteladapter.New(tracer),
//	})
//
// Spans created by the adapter live as children of whatever span is
// already on the context (typical OTel propagation), so handler-side
// tracing nests under mkq.worker.process automatically.
package oteladapter

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/elythia-network/mkq"
)

// Tracer adapts go.opentelemetry.io/otel/trace.Tracer to mkq.Tracer.
type Tracer struct {
	t trace.Tracer
}

// New constructs an adapter over the given OTel tracer. Pass a tracer
// scoped to your application name (typically obtained via
// otel.Tracer("mkq") at app startup).
func New(t trace.Tracer) *Tracer {
	return &Tracer{t: t}
}

// Start opens a span on the given context and returns the child ctx
// plus a Span handle. Attributes passed at Start become the initial
// span attribute set; further SetAttrs calls extend the same span.
func (a *Tracer) Start(ctx context.Context, name string, attrs ...slog.Attr) (context.Context, mkq.Span) {
	ctx, sp := a.t.Start(ctx, name, trace.WithAttributes(toOtelAttrs(attrs)...))
	return ctx, &Span{sp: sp}
}

// Span is the per-operation handle returned by Tracer.Start.
type Span struct {
	sp trace.Span
}

// SetAttrs adds attributes to an in-progress span.
func (s *Span) SetAttrs(attrs ...slog.Attr) {
	s.sp.SetAttributes(toOtelAttrs(attrs)...)
}

// SetError records the error and sets the span status to Error.
// Calling on a span that already ended is a no-op (OTel SDK ignores).
func (s *Span) SetError(err error) {
	if err == nil {
		return
	}
	s.sp.RecordError(err)
	s.sp.SetStatus(codes.Error, err.Error())
}

// End finalises the span. Subsequent SetAttrs / SetError calls are
// dropped by the OTel SDK per its contract.
func (s *Span) End() { s.sp.End() }

// toOtelAttrs converts mkq's slog.Attr slice into OTel attribute
// KeyValues. Type fidelity preserved for string / int / int64 / float64
// / bool. Other types are stringified via slog.Value.String() so the
// span never has dropped attributes — at worst a less-typed
// representation.
func toOtelAttrs(attrs []slog.Attr) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		v := a.Value.Resolve()
		switch v.Kind() {
		case slog.KindString:
			out = append(out, attribute.String(a.Key, v.String()))
		case slog.KindInt64:
			out = append(out, attribute.Int64(a.Key, v.Int64()))
		case slog.KindFloat64:
			out = append(out, attribute.Float64(a.Key, v.Float64()))
		case slog.KindBool:
			out = append(out, attribute.Bool(a.Key, v.Bool()))
		default:
			out = append(out, attribute.String(a.Key, v.String()))
		}
	}
	return out
}
