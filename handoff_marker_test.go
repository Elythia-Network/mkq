package mkq

import "testing"

// TestPausedOrMaxed pins the mirror of BullMQ's isQueuePausedOrMaxed that
// decides whether handoffMarker adds the marker for waiting jobs.
func TestPausedOrMaxed(t *testing.T) {
	cases := []struct {
		name   string
		meta   []any
		active int64
		want   bool
	}{
		{"neither field", []any{nil, nil}, 5, false},
		{"paused", []any{"1", nil}, 0, true},
		{"paused with a free slot", []any{"1", "10"}, 0, true},
		{"below the concurrency limit", []any{nil, "3"}, 2, false},
		{"at the concurrency limit", []any{nil, "3"}, 3, true},
		{"over the concurrency limit", []any{nil, "3"}, 4, true},
		{"fractional concurrency", []any{nil, "2.5"}, 2, false},
		{"fractional concurrency reached", []any{nil, "2.5"}, 3, true},
		{"unparsable concurrency", []any{nil, "x"}, 4, false},
		{"short reply", []any{}, 4, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pausedOrMaxed(tc.meta, tc.active); got != tc.want {
				t.Errorf("pausedOrMaxed(%v, %d) = %v, want %v", tc.meta, tc.active, got, tc.want)
			}
		})
	}
}
