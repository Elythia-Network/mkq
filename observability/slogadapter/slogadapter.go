// Package slogadapter bridges mkq.Logger to *slog.Logger.
//
// Usage:
//
//	import "log/slog"
//	import "github.com/elythia-network/mkq"
//	import "github.com/elythia-network/mkq/observability/slogadapter"
//
//	client, _ := mkq.NewClient(ctx, mkq.Config{
//	    Redis:  redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
//	    Logger: slogadapter.New(slog.Default()),
//	})
//
// The adapter delegates each method to slog.LogAttrs so the structured
// attributes mkq emits land as slog.Attrs (not key/value pair "any"
// args), preserving type information for downstream slog handlers.
package slogadapter

import (
	"context"
	"log/slog"
)

// Logger adapts a *slog.Logger to mkq.Logger.
type Logger struct {
	l *slog.Logger
}

// New constructs an adapter over the given *slog.Logger. If l is nil
// the adapter falls back to slog.Default() so callers can pass
// `slogadapter.New(nil)` to wire mkq into the global slog handler.
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{l: l}
}

// Debug forwards to slog.LogAttrs at DebugLevel. ctx is
// context.Background() because mkq's Logger interface predates
// passing ctx through; downstream slog handlers that key off ctx
// (e.g. trace propagation) should pull it from the global tracer.
func (a *Logger) Debug(msg string, attrs ...slog.Attr) {
	a.l.LogAttrs(context.Background(), slog.LevelDebug, msg, attrs...)
}

// Info forwards to slog.LogAttrs at InfoLevel.
func (a *Logger) Info(msg string, attrs ...slog.Attr) {
	a.l.LogAttrs(context.Background(), slog.LevelInfo, msg, attrs...)
}

// Warn forwards to slog.LogAttrs at WarnLevel.
func (a *Logger) Warn(msg string, attrs ...slog.Attr) {
	a.l.LogAttrs(context.Background(), slog.LevelWarn, msg, attrs...)
}

// Error forwards to slog.LogAttrs at ErrorLevel.
func (a *Logger) Error(msg string, attrs ...slog.Attr) {
	a.l.LogAttrs(context.Background(), slog.LevelError, msg, attrs...)
}
