package mkq_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestWorker_Retention_KeepCompletedTrims verifies that
// WithKeepCompleted(N) bounds the completed ZSET to the most recent N
// entries via BullMQ's removeJobsByMaxCount.
func TestWorker_Retention_KeepCompletedTrims(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const total = 110
	const keep = 100
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)},
			mkq.WithKeepCompleted(keep),
		)
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	},
		mkq.WithConcurrency(4),
		mkq.WithIdlePollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Wait for the wait list to drain and the trim to settle.
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
		activeLen, _ := rdb.LLen(ctx, base+"active").Result()
		completedLen, _ := rdb.ZCard(ctx, base+"completed").Result()
		// 全部処理し終わって、completed が trim 後の値で安定したか。
		return waitLen == 0 && activeLen == 0 && completedLen == int64(keep)
	})

	got, err := rdb.ZCard(ctx, base+"completed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, keep, got, "completed ZSET must be trimmed to keep")
}

// TestWorker_Retention_KeepCompletedZeroRemovesJob verifies that
// WithKeepCompleted(0) (BullMQ removeOnComplete=true) deletes the job
// HASH outright instead of putting it into the completed ZSET.
func TestWorker_Retention_KeepCompletedZeroRemovesJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}, mkq.WithKeepCompleted(0))
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
		activeLen, _ := rdb.LLen(ctx, base+"active").Result()
		exists, _ := rdb.Exists(ctx, base+job.ID).Result()
		// HASH が消滅し、queue keys も空になるまで待つ。
		return waitLen == 0 && activeLen == 0 && exists == 0
	})

	completedLen, _ := rdb.ZCard(ctx, base+"completed").Result()
	assert.Zero(t, completedLen, "completed ZSET must be empty when keep=0")
	exists, _ := rdb.Exists(ctx, base+job.ID).Result()
	assert.EqualValues(t, 0, exists, "job HASH must be removed when keep=0")
}

// TestWorker_Retention_DefaultKeepsAll regression-pins the BullMQ
// default: when neither WithKeepCompleted nor WithKeepFailed is set,
// finished jobs accumulate in their respective ZSET indefinitely.
func TestWorker_Retention_DefaultKeepsAll(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const total = 5
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(total)
	})

	got, err := rdb.ZCard(ctx, base+"completed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, total, got, "default retention must keep all completed jobs")
}

// TestWorker_Retention_KeepCompletedAgeTrims pins the age-based
// retention path. WithKeepCompletedAge(2s) makes each subsequent
// finalisation tick drop entries older than 2 seconds — so jobs
// completed before a 3-second sleep get evicted when a fresh job
// finishes after the sleep.
func TestWorker_Retention_KeepCompletedAgeTrims(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Phase 1: 3 jobs land in completed.
	const oldCount = 3
	for i := range oldCount {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)},
			mkq.WithKeepCompletedAge(2*time.Second),
		)
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(oldCount)
	})

	// Phase 2: wait past the 2s window so the existing entries
	// become eligible for age-trim.
	time.Sleep(2500 * time.Millisecond)

	// Phase 3: enqueue + process one more job. Its moveToFinished
	// runs removeJobsByMaxAge(now, age=2s, ...) which evicts the
	// older entries. completed should end up with just the new job.
	freshJob, err := queue.Add(ctx, testPayload{Inbox: "fresh"},
		mkq.WithKeepCompletedAge(2*time.Second),
	)
	require.NoError(t, err)

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", freshJob.ID).Result()
		return v > 0
	})

	got, err := rdb.ZCard(ctx, base+"completed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, got, "age-trim must keep only entries newer than the window")
}

// TestWorker_Retention_KeepFailedTrims is the failure-side analogue
// of KeepCompletedTrims.
func TestWorker_Retention_KeepFailedTrims(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const total = 5
	const keep = 2
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)},
			mkq.WithKeepFailed(keep),
		)
		require.NoError(t, err)
	}

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("boom")
	},
		mkq.WithConcurrency(2),
		mkq.WithIdlePollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
		activeLen, _ := rdb.LLen(ctx, base+"active").Result()
		failedLen, _ := rdb.ZCard(ctx, base+"failed").Result()
		return waitLen == 0 && activeLen == 0 && failedLen == int64(keep)
	})

	got, err := rdb.ZCard(ctx, base+"failed").Result()
	require.NoError(t, err)
	assert.EqualValues(t, keep, got, "failed ZSET must be trimmed to keep")
}
