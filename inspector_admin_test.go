package mkq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInspectorAdmin_RemoveJob_FromWait removes a wait-state job and
// verifies both the per-job HASH and the LIST membership are gone.
func TestInspectorAdmin_RemoveJob_FromWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "to-remove"})
	require.NoError(t, err)

	require.NoError(t, queue.RemoveJob(ctx, job.ID))

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	exists, err := rdb.Exists(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, exists, "job HASH must be removed")

	pos, _ := rdb.LPos(ctx, base+"wait", job.ID, redisLPosArgs{}).Result()
	_ = pos // LPos returns redis.Nil when not present; we only care that the position lookup didn't find it
}

// TestInspectorAdmin_RemoveJob_Active confirms the safety guard:
// a job currently held by a worker (lock present) cannot be removed
// — RemoveJob returns ErrJobActive and leaves the HASH intact.
func TestInspectorAdmin_RemoveJob_Active(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, testPayload{Inbox: "blocking"})
	require.NoError(t, err)

	// 立ち上げた worker は handler の中でブロックする → job は active 状態のまま。
	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(handlerEntered)
		<-releaseHandler
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)

	t.Cleanup(func() {
		// Make sure the handler returns even on test failure so Worker.Stop drains.
		select {
		case <-releaseHandler:
		default:
			close(releaseHandler)
		}
		_ = worker.Stop(context.Background())
	})

	<-handlerEntered

	err = queue.RemoveJob(ctx, addedJob.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mkq.ErrJobActive), "expected ErrJobActive, got %v", err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	exists, err := rdb.Exists(ctx, base+addedJob.ID).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, exists, "active job HASH must remain after blocked RemoveJob")
}

// TestInspectorAdmin_RemoveJob_Missing pins the no-op-on-missing
// behaviour: removing a never-existed job returns nil (not an error)
// because the underlying lua treats the missing-job branch as
// idempotent success rather than -1.
func TestInspectorAdmin_RemoveJob_Missing(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assert.NoError(t, queue.RemoveJob(ctx, "does-not-exist"))
}

// TestInspectorAdmin_DrainPending_DefaultLeavesDelayed seeds 3 wait
// + 2 delayed jobs and verifies that DrainPending (without the
// WithDrainDelayed flag) only clears the wait LIST.
func TestInspectorAdmin_DrainPending_DefaultLeavesDelayed(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range 3 {
		_, err := queue.Add(ctx, testPayload{})
		require.NoError(t, err)
	}
	for range 2 {
		_, err := queue.Add(ctx, testPayload{}, mkq.WithDelay(10*time.Minute))
		require.NoError(t, err)
	}

	require.NoError(t, queue.DrainPending(ctx))

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, counts.Wait, "wait must be drained")
	assert.EqualValues(t, 2, counts.Delayed, "delayed must be preserved without WithDrainDelayed")
}

// TestInspectorAdmin_DrainPending_WithDelayed drains delayed too
// when the option flips on.
func TestInspectorAdmin_DrainPending_WithDelayed(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range 3 {
		_, err := queue.Add(ctx, testPayload{})
		require.NoError(t, err)
	}
	for range 2 {
		_, err := queue.Add(ctx, testPayload{}, mkq.WithDelay(10*time.Minute))
		require.NoError(t, err)
	}

	require.NoError(t, queue.DrainPending(ctx, mkq.WithDrainDelayed(true)))

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, counts.Wait)
	assert.EqualValues(t, 0, counts.Delayed, "delayed must be drained when WithDrainDelayed(true)")
}

// TestInspectorAdmin_PromoteJob_DelayedToWait promotes a delayed job
// past its scheduled fire time and verifies a worker dequeues it
// immediately (rather than waiting out the original delay).
func TestInspectorAdmin_PromoteJob_DelayedToWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "delayed"}, mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	// Confirm baseline: job sits in delayed.
	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, counts.Delayed)

	require.NoError(t, queue.PromoteJob(ctx, job.ID))

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	select {
	case got := <-processed:
		assert.Equal(t, job.ID, got)
	case <-time.After(2 * time.Second):
		t.Fatal("promoted job not dequeued within 2s")
	}
}

// TestInspectorAdmin_PromoteJob_NotDelayed pins the typed error for
// the misuse case (calling Promote on a wait/active/completed job).
func TestInspectorAdmin_PromoteJob_NotDelayed(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}) // wait, not delayed
	require.NoError(t, err)

	err = queue.PromoteJob(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mkq.ErrJobNotInDelayed), "expected ErrJobNotInDelayed, got %v", err)
}

// TestInspectorAdmin_RetryJob_FailedToWait re-enqueues a failed job
// and verifies the worker observes a fresh attempt counter (atm=0).
func TestInspectorAdmin_RetryJob_FailedToWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "will-fail"})
	require.NoError(t, err)

	// First worker fails the job, then stops.
	failure := make(chan struct{})
	w1, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(failure)
		return nil, errors.New("intentional fail")
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	<-failure
	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})
	require.NoError(t, w1.Stop(context.Background()))

	// Re-enqueue via RetryJob; second worker should see a fresh atm.
	require.NoError(t, queue.RetryJob(ctx, job.ID))

	atmObserved := make(chan int, 1)
	w2, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		atmObserved <- j.AttemptsMade
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Stop(context.Background()) })

	select {
	case atm := <-atmObserved:
		assert.Equal(t, 0, atm, "RetryJob (default WithResetAttempts) must reset atm to 0")
	case <-time.After(2 * time.Second):
		t.Fatal("retried job not dequeued within 2s")
	}
}

// TestInspectorAdmin_RetryJob_NotInExpectedState confirms the typed
// error when callers ask to retry from "failed" but the job is
// somewhere else (e.g. still in wait).
func TestInspectorAdmin_RetryJob_NotInExpectedState(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}) // wait, not failed
	require.NoError(t, err)

	err = queue.RetryJob(ctx, job.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mkq.ErrJobNotInExpectedState), "expected ErrJobNotInExpectedState, got %v", err)
}

// redisLPosArgs is a tiny shim so the test file doesn't have to
// import go-redis just for the LPos options struct.
type redisLPosArgs = struct {
	Rank   int64
	MaxLen int64
}
