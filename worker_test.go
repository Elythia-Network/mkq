package mkq_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// waitFor polls cond every step until it returns true or ctx is done.
// Integration tests against Redis are inherently asynchronous; pinning a
// fixed sleep gives flaky CI.
func waitFor(t *testing.T, ctx context.Context, step time.Duration, cond func() bool) {
	t.Helper()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waitFor: timed out: %v", ctx.Err())
		case <-time.After(step):
		}
	}
}

func TestWorker_Process_Success(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, testPayload{Inbox: "https://x", Body: "ok"})
	require.NoError(t, err)

	type observed struct {
		jobID string
		data  testPayload
	}
	var got atomic.Value
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		got.Store(observed{jobID: j.ID, data: j.Data})
		return map[string]any{"ok": true}, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", addedJob.ID).Result()
		return v > 0
	})

	o, _ := got.Load().(observed)
	assert.Equal(t, addedJob.ID, o.jobID)
	assert.Equal(t, "https://x", o.data.Inbox)

	h, err := rdb.HGetAll(ctx, base+addedJob.ID).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, h["processedOn"], "processedOn must be set")
	assert.NotEmpty(t, h["finishedOn"], "finishedOn must be set")
	require.NotEmpty(t, h["returnvalue"])
	var ret map[string]any
	require.NoError(t, json.Unmarshal([]byte(h["returnvalue"]), &ret))
	assert.Equal(t, true, ret["ok"])

	// The lock key must be released by moveToFinished.
	exists, err := rdb.Exists(ctx, base+addedJob.ID+":lock").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, exists, "lock key must be released after finish")
}

func TestWorker_Process_HandlerError(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("boom")
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", addedJob.ID).Result()
		return v > 0
	})

	h, err := rdb.HGetAll(ctx, base+addedJob.ID).Result()
	require.NoError(t, err)
	// BullMQ stores failedReason as a plain string (matching err.message
	// in the TS client), not JSON-encoded.
	assert.Equal(t, "boom", h["failedReason"])
}

func TestWorker_Process_Panic(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		panic("kaboom")
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", addedJob.ID).Result()
		return v > 0
	})

	h, err := rdb.HGetAll(ctx, base+addedJob.ID).Result()
	require.NoError(t, err)
	reason := h["failedReason"]
	assert.Contains(t, reason, "panic: kaboom")
	assert.Contains(t, reason, "goroutine ", "stack trace must be captured")
}

func TestWorker_Process_Concurrency(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const N = 8
	for i := range N {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}

	var done atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		// 並行性を観測しやすくするため軽い擬似処理を入れる。
		time.Sleep(50 * time.Millisecond)
		done.Add(1)
		return nil, nil
	},
		mkq.WithConcurrency(N),
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(N)
	})
	assert.EqualValues(t, N, done.Load(), "every handler must have run")
}

// TestWorker_Process_SerialJobsOnSingleSlot regression-tests two
// behaviours that PR #9's first round broke:
//
//  1. Without WithConcurrency(1) gating, a single dispatch loop must
//     still process N jobs sequentially without skipping any. Earlier
//     code spawned per-job goroutines, which masked correctness bugs
//     by parallelising around them.
//
//  2. moveToFinished's vendored Lua needs maxMetricsSize="" or it
//     crashes via collectMetrics on the *second* job (tonumber(nil)
//     in math.min). Running multiple jobs through one slot exercises
//     this path.
func TestWorker_Process_SerialJobsOnSingleSlot(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const N = 5
	for i := range N {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		seen.Add(1)
		return nil, nil
	},
		mkq.WithConcurrency(1),
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(N)
	})
	assert.EqualValues(t, N, seen.Load())
}

func TestWorker_Process_PriorityOrderingWithinPrioritized(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// BullMQ's vendored moveToActive drains the wait list before the
	// prioritized ZSET, so a "priority job vs standard job" race is
	// not the right ordering test (standard jobs always win when wait
	// is non-empty). The real priority semantic — that smaller numeric
	// priority runs first — applies *within* the prioritized bucket.
	low, err := queue.Add(ctx, testPayload{Inbox: "low"}, mkq.WithPriority(10))
	require.NoError(t, err)
	high, err := queue.Add(ctx, testPayload{Inbox: "high"}, mkq.WithPriority(1))
	require.NoError(t, err)

	order := make(chan string, 2)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		order <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	first := receiveOrFail(t, ctx, order)
	second := receiveOrFail(t, ctx, order)

	assert.Equal(t, high.ID, first, "priority=1 should run before priority=10")
	assert.Equal(t, low.ID, second)
}

// TestWorker_Process_DelayedJobRunsAfterDelay pins that delayed jobs
// (added via WithDelay) get picked up by the worker once their delay
// elapses. mkq doesn't run a separate scheduler — promotion is
// performed inline by moveToActive-11.lua's promoteDelayedJobs call
// every dequeue tick — so this test is the regression guarantee that
// the polling loop + vendored Lua combination still works end-to-end.
func TestWorker_Process_DelayedJobRunsAfterDelay(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const delay = 80 * time.Millisecond
	addedAt := time.Now()
	job, err := queue.Add(ctx, testPayload{Inbox: "delayed"},
		mkq.WithDelay(delay),
	)
	require.NoError(t, err)

	ranAt := make(chan time.Time, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		ranAt <- time.Now()
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	got := receiveOrFail(t, ctx, ranAt)
	elapsed := got.Sub(addedAt)
	assert.GreaterOrEqual(t, elapsed, delay-10*time.Millisecond,
		"handler must not run before the delay (got elapsed=%v)", elapsed)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})
}

func TestWorker_Stop_DrainsInflight(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	started := make(chan struct{})
	finished := make(chan struct{})
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(started)
		// Stop が呼ばれた後でも処理を完走させる。
		time.Sleep(300 * time.Millisecond)
		close(finished)
		return "done", nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)

	receiveOrFail(t, ctx, started)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, worker.Stop(stopCtx))

	// 完走したことを確認 (Stop が in-flight を待ったか)。
	select {
	case <-finished:
	default:
		t.Fatal("Stop returned before in-flight handler finished")
	}

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	v, err := rdb.ZScore(ctx, base+"completed", job.ID).Result()
	require.NoError(t, err)
	assert.Greater(t, v, float64(0))
}

func receiveOrFail[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatalf("expected channel send, got ctx done: %v", ctx.Err())
		var zero T
		return zero
	}
}

// Sanity: assert that the dispatch path is entered only via Process —
// callers should not be tempted to peek at unexported fields.
func TestWorker_Process_RejectsNilQueue(t *testing.T) {
	t.Parallel()
	_, err := mkq.Process(nil, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	})
	require.Error(t, err)
}

func TestWorker_Process_RejectsNilHandler(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")
	_, err := mkq.Process(queue, mkq.Handler[testPayload](nil))
	require.Error(t, err)
}
