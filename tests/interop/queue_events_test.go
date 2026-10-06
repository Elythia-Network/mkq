//go:build interop

package interop_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInterop_QueueEvents_ObservesBullMQTSEmissions pins that mkq's
// QueueEvents subscriber sees events emitted by a BullMQ TS worker
// processing a job that mkq enqueued. This is the cross-language
// version of the events-side wire compatibility — the same XADD
// payload BullMQ TS produces must decode to mkq's typed Events.
func TestInterop_QueueEvents_ObservesBullMQTSEmissions(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "deliver"

	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	// Subscribe FIRST so the BullMQ-emitted events land within our
	// `$`-anchored window.
	qe := mkq.NewQueueEvents(queue)
	t.Cleanup(qe.Close)

	type capture struct {
		completed *mkq.CompletedEvent
		failed    *mkq.FailedEvent
	}
	var got capture
	done := make(chan struct{})
	// errStop is a sentinel returned by the handler after the first
	// completed/failed capture so Subscribe exits immediately. This
	// removes any post-capture concurrent write to `got` from the
	// subscriber goroutine while the test's main goroutine reads it.
	errStop := errors.New("interop: stop after first terminal event")
	go func() {
		err := qe.Subscribe(context.Background(), func(ev mkq.Event) error {
			switch e := ev.(type) {
			case mkq.CompletedEvent:
				got.completed = &e
				close(done)
				return errStop
			case mkq.FailedEvent:
				got.failed = &e
				close(done)
				return errStop
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			t.Errorf("Subscribe returned unexpected error: %v", err)
		}
	}()
	// Let the BLOCK-XREAD register before BullMQ TS starts emitting.
	time.Sleep(100 * time.Millisecond)

	startNodeWorker(t, prefix, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "x", Body: "hello"})
	require.NoError(t, err)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("no completed/failed event from BullMQ TS within timeout")
	}

	require.NotNil(t, got.completed, "BullMQ TS should have emitted a completed event")
	assert.Equal(t, job.ID, got.completed.JobID)
	assert.NotEmpty(t, got.completed.ReturnValue,
		"completed event must carry the BullMQ TS handler's return value")
}
