package mkq_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestWorker_Retry_FixedBackoffExhausts pins the basic retry contract:
// WithAttempts(N) + Fixed(d) makes the worker run the handler N times
// (failing each time) before settling the job in failed.
func TestWorker_Retry_FixedBackoffExhausts(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(3),
		mkq.WithBackoff(mkq.FixedBackoff(20*time.Millisecond)),
	)
	require.NoError(t, err)

	var attempts atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		n := attempts.Add(1)
		return nil, fmt.Errorf("attempt %d failed", n)
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	assert.EqualValues(t, 3, attempts.Load(), "handler must run exactly attempts times")

	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.Equal(t, "3", h["atm"], "atm must be incremented to 3")
	assert.Equal(t, "attempt 3 failed", h["failedReason"], "final failedReason persists")
}

// TestWorker_Retry_ExponentialBackoffSchedules verifies the second
// retry happens after exponential backoff (delay * 2^(n-1)).
func TestWorker_Retry_ExponentialBackoffSchedules(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(3),
		mkq.WithBackoff(mkq.ExponentialBackoff(50*time.Millisecond)),
	)
	require.NoError(t, err)

	var times []int64
	mu := make(chan struct{}, 1)
	mu <- struct{}{}
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		<-mu
		times = append(times, time.Now().UnixMilli())
		mu <- struct{}{}
		return nil, errors.New("nope")
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	require.Len(t, times, 3, "expected 3 attempts")
	gap1 := times[1] - times[0]
	gap2 := times[2] - times[1]
	// 1st retry: 50ms * 2^0 = 50ms; 2nd retry: 50ms * 2^1 = 100ms.
	// 寛容な下限 (poll/scheduling 揺らぎを吸収) でassertする。
	assert.GreaterOrEqual(t, gap1, int64(40), "first retry should wait ~50ms (got %dms)", gap1)
	assert.GreaterOrEqual(t, gap2, int64(85), "second retry should wait ~100ms (got %dms)", gap2)
	// 上限は緩く (CIタイマー揺らぎ吸収): doublingが効いていることだけ確認。
	assert.Greater(t, gap2, gap1, "second gap should exceed first")
}

// TestWorker_Retry_CustomBackoffSchedules verifies the end-to-end wire
// path for a custom backoff strategy: CustomBackoff() on Add marks the
// job {"type":"custom"}, WithBackoffStrategy registers the Go-side
// computation, and the worker honours the returned per-attempt delay
// when re-enqueuing onto the delayed ZSET. This is the path mk-go uses
// to reproduce Misskey's httpRelatedBackoff.
func TestWorker_Retry_CustomBackoffSchedules(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(3),
		mkq.WithBackoff(mkq.CustomBackoff()),
	)
	require.NoError(t, err)

	var times []int64
	mu := make(chan struct{}, 1)
	mu <- struct{}{}
	// Strategy: attempt 1 -> 40ms, attempt 2 -> 120ms (distinct, growing,
	// and not matching the built-in exponential doubling so we know the
	// custom func actually drives the schedule).
	strategy := func(attemptsMade int) time.Duration {
		return time.Duration(attemptsMade*attemptsMade) * 40 * time.Millisecond
	}
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		<-mu
		times = append(times, time.Now().UnixMilli())
		mu <- struct{}{}
		return nil, errors.New("nope")
	}, mkq.WithIdlePollInterval(10*time.Millisecond), mkq.WithBackoffStrategy(strategy))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	require.Len(t, times, 3, "expected 3 attempts")
	gap1 := times[1] - times[0]
	gap2 := times[2] - times[1]
	// 1st retry: strategy(1) = 1*40 = 40ms; 2nd retry: strategy(2) = 4*40 = 160ms.
	// 寛容な下限 (poll/scheduling 揺らぎを吸収) でassertする。
	assert.GreaterOrEqual(t, gap1, int64(30), "first retry should wait ~40ms (got %dms)", gap1)
	assert.GreaterOrEqual(t, gap2, int64(140), "second retry should wait ~160ms (got %dms)", gap2)
	assert.Greater(t, gap2, gap1, "second gap should exceed first (custom strategy grows)")

	// opts.backoff must persist as the BullMQ custom wire shape so a
	// foreign worker / dashboard reads {"type":"custom"}.
	h, err := rdb.HGet(ctx, base+job.ID, "opts").Result()
	require.NoError(t, err)
	assert.Contains(t, h, `"type":"custom"`, "opts.backoff must persist type=custom")
}

// TestWorker_Retry_NoAttemptsDefaultIsOneShot ensures the existing
// no-WithAttempts behaviour (1 attempt -> failed) is preserved.
func TestWorker_Retry_NoAttemptsDefaultIsOneShot(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}) // no WithAttempts
	require.NoError(t, err)

	var attempts atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		attempts.Add(1)
		return nil, errors.New("boom")
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	assert.EqualValues(t, 1, attempts.Load(), "default (no WithAttempts) must run exactly once")
}

// TestWorker_Retry_UnrecoverableSkipsRetry verifies that wrapping
// ErrUnrecoverable bypasses retry even when WithAttempts allows more.
func TestWorker_Retry_UnrecoverableSkipsRetry(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.FixedBackoff(10*time.Millisecond)),
	)
	require.NoError(t, err)

	var attempts atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		attempts.Add(1)
		return nil, fmt.Errorf("hard fail: %w", mkq.ErrUnrecoverable)
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	assert.EqualValues(t, 1, attempts.Load(), "unrecoverable error must bypass retry")
	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.Contains(t, h["failedReason"], "hard fail")
}

// TestWorker_Retry_UnrecoverableSuppressesRetriesExhaustedEvent pins
// a BullMQ wire-format detail: when WithAttempts(N) is configured but
// the handler short-circuits via ErrUnrecoverable on the first run,
// the events stream must NOT carry a `retries-exhausted` entry.
//
// BullMQ TS gates the event on `attemptsMade >= attempts`; if the
// worker omits the `attempts` opt forwarded into moveToFinished's
// Lua, the gate degrades to `1 >= 0` and emits spuriously, breaking
// bull-board / foreign QueueEvents consumers.
func TestWorker_Retry_UnrecoverableSuppressesRetriesExhaustedEvent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.FixedBackoff(10*time.Millisecond)),
	)
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, fmt.Errorf("hard fail: %w", mkq.ErrUnrecoverable)
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	// XRANGEで全eventを取り、retries-exhausted が無いことを確認。
	entries, err := rdb.XRange(ctx, base+"events", "-", "+").Result()
	require.NoError(t, err)
	for _, e := range entries {
		ev, _ := e.Values["event"].(string)
		assert.NotEqual(t, "retries-exhausted", ev,
			"retries-exhausted must not be emitted when WithAttempts(5) is unspent (event=%v jobId=%v)", e.Values["event"], e.Values["jobId"])
	}
}

// TestWorker_Retry_StacktraceFieldShape pins the BullMQ wire-shape
// for the stacktrace HASH field: a JSON array of strings.
func TestWorker_Retry_StacktraceFieldShape(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("kaput")
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	require.NotEmpty(t, h["stacktrace"], "stacktrace must be written on failure")
	assert.True(t, h["stacktrace"][0] == '[' && h["stacktrace"][len(h["stacktrace"])-1] == ']',
		"stacktrace should be a JSON array, got: %s", h["stacktrace"])
	assert.Contains(t, h["stacktrace"], "kaput")
}
