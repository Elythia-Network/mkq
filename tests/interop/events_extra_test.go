//go:build interop

// Event-stream coverage beyond completed/failed (which are pinned by
// queue_events_test.go in PR #31). Two scenarios:
//
//   1. mkq emits a variety of events (delayed, removed, progress);
//      a BullMQ TS QueueEvents listener subscribes and asserts each
//      event arrives. Direction: mkq→BullMQ (the inverse of PR #31).
//
//   2. mkq enqueues + processes; mkq's own QueueEvents subscriber
//      observes the variety. Direction: in-process, but exercises
//      the same wire format the listener does.
//
// The cross-language "BullMQ reads mkq" path is the one that wasn't
// covered before.

package interop_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInterop_Events_BullMQReadsMkqEmissions confirms a BullMQ TS
// QueueEvents listener observes the wire-format events mkq's
// vendored Lua emits when an mkq client adds + processes jobs. Pins
// the "inverse direction" half of the events stream interop matrix.
func TestInterop_Events_BullMQReadsMkqEmissions(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "evx"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Subscribe BEFORE any enqueue so the BullMQ listener catches
	// added/waiting/active/completed for the upcoming run.
	events := startEventsListener(t, prefix, queueName,
		"added", "waiting", "active", "completed", "delayed", "removed")
	// Tiny pause so the listener's XREAD position settles.
	time.Sleep(100 * time.Millisecond)

	// Path 1: a happy-path job exercises added → waiting → active → completed.
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[interopPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	immediateJob, err := queue.Add(ctx, interopPayload{Inbox: "happy"})
	require.NoError(t, err)

	// Path 2: a delayed job exercises the `delayed` event.
	delayedJob, err := queue.Add(ctx, interopPayload{Inbox: "later"}, mkq.WithDelay(10*time.Minute))
	require.NoError(t, err)

	// Path 3: removing a wait/delayed job triggers the `removed` event.
	require.NoError(t, queue.RemoveJob(ctx, delayedJob.ID))

	// Path 1/2/3 で発火させた event は全て assertion 対象に入れる。
	// これがないと delayed Add / Remove の setup が「動いただけ」で
	// wire compat の検証になっていない (PR #52 round-1 review 指摘)。
	required := map[string]bool{
		"added":     false,
		"waiting":   false,
		"active":    false,
		"completed": false,
		"delayed":   false,
		"removed":   false,
	}
	deadline := time.After(8 * time.Second)
	allSeen := func() bool {
		for _, v := range required {
			if !v {
				return false
			}
		}
		return true
	}
	for !allSeen() {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("events listener closed before all required events seen; got %v", required)
			}
			name, _ := ev["event"].(string)
			if _, ok := required[name]; ok {
				required[name] = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events; got %v", required)
		}
	}
	for name, ok := range required {
		assert.True(t, ok, "BullMQ listener should observe `%s` event", name)
	}
	_ = immediateJob
}
