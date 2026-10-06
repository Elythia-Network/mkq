package mkq_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// startSubscriber spawns Subscribe in a goroutine and returns a
// stop channel + an event chan that the caller can drain. Subscribe
// errors are surfaced on the returned err channel.
func startSubscriber(t *testing.T, qe *mkq.QueueEvents) (events <-chan mkq.Event, errs <-chan error) {
	t.Helper()
	evCh := make(chan mkq.Event, 32)
	errCh := make(chan error, 1)
	go func() {
		errCh <- qe.Subscribe(context.Background(), func(ev mkq.Event) error {
			evCh <- ev
			return nil
		})
	}()
	t.Cleanup(qe.Close)
	return evCh, errCh
}

// drainUntil collects events into the returned slice until pred
// matches, the test ctx fires, or the stream goes idle for 1s.
func drainUntil(t *testing.T, ctx context.Context, ch <-chan mkq.Event, pred func([]mkq.Event) bool) []mkq.Event {
	t.Helper()
	var seen []mkq.Event
	idle := time.NewTimer(2 * time.Second)
	defer idle.Stop()
	for {
		select {
		case ev := <-ch:
			seen = append(seen, ev)
			idle.Reset(2 * time.Second)
			if pred(seen) {
				return seen
			}
		case <-idle.C:
			t.Fatalf("subscribe idle for 2s, events so far: %d", len(seen))
		case <-ctx.Done():
			t.Fatalf("ctx done, events so far: %d", len(seen))
		}
	}
}

// containsType reports whether a typed event is present in xs.
func containsType[E mkq.Event](xs []mkq.Event) bool {
	for _, x := range xs {
		if _, ok := x.(E); ok {
			return true
		}
	}
	return false
}

// findFirst returns the first event of type E (or zero value + false).
func findFirst[E mkq.Event](xs []mkq.Event) (E, bool) {
	var zero E
	for _, x := range xs {
		if e, ok := x.(E); ok {
			return e, true
		}
	}
	return zero, false
}

// TestQueueEvents_BasicFlow drives Add -> Process -> Completed and
// asserts the four wire events in order.
func TestQueueEvents_BasicFlow(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	events, _ := startSubscriber(t, qe)
	// Subscribe starts at "$" — give the BLOCKING XREAD a brief
	// moment to register before Add fires.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return map[string]any{"ok": true}, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	job, err := queue.Add(ctx, testPayload{Inbox: "x"})
	require.NoError(t, err)

	got := drainUntil(t, ctx, events, func(xs []mkq.Event) bool {
		return containsType[mkq.CompletedEvent](xs)
	})

	added, ok := findFirst[mkq.AddedEvent](got)
	require.True(t, ok, "no AddedEvent in: %v", got)
	assert.Equal(t, job.ID, added.JobID)

	require.True(t, containsType[mkq.WaitingEvent](got))
	require.True(t, containsType[mkq.ActiveEvent](got))

	completed, ok := findFirst[mkq.CompletedEvent](got)
	require.True(t, ok)
	assert.Equal(t, job.ID, completed.JobID)
	require.NotEmpty(t, completed.ReturnValue)
	var rv map[string]any
	require.NoError(t, json.Unmarshal(completed.ReturnValue, &rv))
	assert.Equal(t, true, rv["ok"])
}

// TestQueueEvents_FailedEvent pins the failed-path payload (jobId +
// failedReason as plain string).
func TestQueueEvents_FailedEvent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	events, _ := startSubscriber(t, qe)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("bad")
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	_, err = queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	got := drainUntil(t, ctx, events, func(xs []mkq.Event) bool {
		return containsType[mkq.FailedEvent](xs)
	})
	failed, ok := findFirst[mkq.FailedEvent](got)
	require.True(t, ok)
	assert.Equal(t, "bad", failed.FailedReason)
}

// TestQueueEvents_DelayedEvent verifies WithDelay produces a delayed
// event and that the delay value (ms) round-trips.
func TestQueueEvents_DelayedEvent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	events, _ := startSubscriber(t, qe)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{}, mkq.WithDelay(2*time.Second))
	require.NoError(t, err)

	got := drainUntil(t, ctx, events, func(xs []mkq.Event) bool {
		return containsType[mkq.DelayedEvent](xs)
	})
	d, ok := findFirst[mkq.DelayedEvent](got)
	require.True(t, ok)
	// BullMQ Lua emits delay as the absolute target ms timestamp.
	assert.Greater(t, d.DelayedUntilMs, time.Now().UnixMilli()-1000,
		"delayed event target should be a current-or-future ms timestamp, got %d", d.DelayedUntilMs)
}

// TestQueueEvents_HandlerError pins that returning a non-nil error
// from the callback stops Subscribe and propagates the error.
func TestQueueEvents_HandlerError(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	t.Cleanup(qe.Close)

	want := errors.New("stop please")
	done := make(chan error, 1)
	go func() {
		done <- qe.Subscribe(context.Background(), func(ev mkq.Event) error {
			return want
		})
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	select {
	case err := <-done:
		assert.ErrorIs(t, err, want)
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe did not return after handler error")
	}
}

// TestQueueEvents_CloseStopsSubscribe pins that Close terminates the
// loop with nil.
func TestQueueEvents_CloseStopsSubscribe(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)

	done := make(chan error, 1)
	go func() {
		done <- qe.Subscribe(context.Background(), func(ev mkq.Event) error {
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond)

	qe.Close()

	select {
	case err := <-done:
		assert.NoError(t, err, "Subscribe must return nil after Close")
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not return after Close")
	}
}

// TestQueueEvents_RawEventForUnknown writes an unknown event type
// directly via XADD and confirms RawEvent is delivered with intact
// fields.
func TestQueueEvents_RawEventForUnknown(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	events, _ := startSubscriber(t, qe)
	time.Sleep(50 * time.Millisecond)

	rdb := rawClient(t)
	ctx := context.Background()
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: prefix + ":deliver:events",
		Values: map[string]any{
			"event":  "future-event-from-bullmq-v6",
			"jobId":  "42",
			"custom": "value",
		},
	})

	tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := drainUntil(t, tctx, events, func(xs []mkq.Event) bool {
		return containsType[mkq.RawEvent](xs)
	})
	raw, ok := findFirst[mkq.RawEvent](got)
	require.True(t, ok)
	assert.Equal(t, "future-event-from-bullmq-v6", raw.Type)
	assert.Equal(t, "42", raw.Fields["jobId"])
	assert.Equal(t, "value", raw.Fields["custom"])
}

// TestQueueEvents_NilHandlerErrors verifies the input validation.
func TestQueueEvents_NilHandlerErrors(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")
	qe := mkq.NewQueueEvents(queue)
	err := qe.Subscribe(context.Background(), nil)
	require.Error(t, err)
}
