package slogadapter

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/elythia-network/mkq"
)

// Compile-time assertion: *Logger must satisfy mkq.Logger.
var _ mkq.Logger = (*Logger)(nil)

// TestLogger_ForwardsLevelsAndAttrs runs each level method and
// verifies the *slog.Logger sees the right level + attribute set.
// Uses slog.NewJSONHandler so attribute types survive (textHandler
// stringifies; we want to see attr type fidelity).
func TestLogger_ForwardsLevelsAndAttrs(t *testing.T) {
	cases := []struct {
		name    string
		invoke  func(l *Logger)
		wantLvl string
		wantMsg string
		wantKey string
		wantVal any
	}{
		{
			name: "debug",
			invoke: func(l *Logger) {
				l.Debug("dbg", slog.String("k", "v"))
			},
			wantLvl: "DEBUG", wantMsg: "dbg", wantKey: "k", wantVal: "v",
		},
		{
			name: "info",
			invoke: func(l *Logger) {
				l.Info("nfo", slog.Int("n", 7))
			},
			wantLvl: "INFO", wantMsg: "nfo", wantKey: "n", wantVal: float64(7),
		},
		{
			name: "warn",
			invoke: func(l *Logger) {
				l.Warn("wrn", slog.String("k", "v"))
			},
			wantLvl: "WARN", wantMsg: "wrn", wantKey: "k", wantVal: "v",
		},
		{
			name: "error",
			invoke: func(l *Logger) {
				l.Error("err", slog.String("k", "v"))
			},
			wantLvl: "ERROR", wantMsg: "err", wantKey: "k", wantVal: "v",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
			tc.invoke(New(slog.New(h)))
			line := strings.TrimSpace(buf.String())
			if line == "" {
				t.Fatalf("expected log output, got empty buffer")
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("unmarshal log line: %v (line=%q)", err, line)
			}
			if rec["level"] != tc.wantLvl {
				t.Errorf("level = %v, want %v", rec["level"], tc.wantLvl)
			}
			if rec["msg"] != tc.wantMsg {
				t.Errorf("msg = %v, want %v", rec["msg"], tc.wantMsg)
			}
			if rec[tc.wantKey] != tc.wantVal {
				t.Errorf("attr %q = %v (%T), want %v (%T)",
					tc.wantKey, rec[tc.wantKey], rec[tc.wantKey], tc.wantVal, tc.wantVal)
			}
		})
	}
}

// TestNew_NilFallsBackToDefault verifies New(nil) uses slog.Default()
// so callers can wire to the global handler without keeping a handle.
func TestNew_NilFallsBackToDefault(t *testing.T) {
	a := New(nil)
	if a.l == nil {
		t.Fatalf("New(nil) must fall back to slog.Default(), got nil")
	}
}
