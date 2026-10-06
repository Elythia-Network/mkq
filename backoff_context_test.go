package mkq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// retryAfterError is the shape an executor wraps a rate-limit response
// in: the remote told us when to come back, and the backoff strategy
// should be able to honour it.
type retryAfterError struct{ after time.Duration }

func (e *retryAfterError) Error() string { return "rate limited" }

// TestWorker_BackoffContext_SeesTheJobAndTheError pins the wiring end
// to end: a job that fails with a typed error reaches the registered
// strategy with the job id, the job name, the attempt count and the
// error the handler actually returned. That the chosen delay is
// actually scheduled is covered separately by
// TestWorker_BackoffContext_DelayIsHonoured — at 40ms this test cannot
// tell a scheduled retry from an immediate one.
func TestWorker_BackoffContext_SeesTheJobAndTheError(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithJobName("deliver-note"),
		mkq.WithAttempts(2),
		mkq.WithBackoff(mkq.CustomBackoff()),
	)
	require.NoError(t, err)

	var mu sync.Mutex
	var seen []mkq.BackoffContext

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, &retryAfterError{after: 40 * time.Millisecond}
	},
		mkq.WithIdlePollInterval(10*time.Millisecond),
		mkq.WithBackoffStrategyFunc(func(bc mkq.BackoffContext) time.Duration {
			mu.Lock()
			seen = append(seen, bc)
			mu.Unlock()

			var rl *retryAfterError
			if errors.As(bc.Err, &rl) {
				return rl.after
			}
			return time.Hour // 取り違えたらテストがタイムアウトで落ちる
		}),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 1, "the strategy runs once, for the single retry between two attempts")

	bc := seen[0]
	assert.Equal(t, job.ID, bc.JobID)
	assert.Equal(t, "deliver-note", bc.Name, "the job name, not the queue name")
	assert.Equal(t, 1, bc.AttemptsMade, "post-bump count: this was attempt 1")
	assert.Equal(t, "custom", bc.BackoffType)

	var rl *retryAfterError
	require.True(t, errors.As(bc.Err, &rl), "the handler error must reach the strategy, got %v", bc.Err)
	assert.Equal(t, 40*time.Millisecond, rl.after)
}

// The older attempt-count-only form keeps working untouched.
func TestWorker_BackoffContext_AttemptOnlyFormStillWorks(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(2),
		mkq.WithBackoff(mkq.CustomBackoff()),
	)
	require.NoError(t, err)

	var mu sync.Mutex
	var attempts []int

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("nope")
	},
		mkq.WithIdlePollInterval(10*time.Millisecond),
		mkq.WithBackoffStrategy(func(attemptsMade int) time.Duration {
			mu.Lock()
			attempts = append(attempts, attemptsMade)
			mu.Unlock()
			return 30 * time.Millisecond
		}),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{1}, attempts)
}

// TestWorker_BackoffContext_DelayIsHonoured checks that the duration the
// strategy returns is the one the retry actually waits, rather than the
// call merely happening. The delay is large enough to be distinguishable
// from an immediate re-enqueue at the granularity a test can observe.
func TestWorker_BackoffContext_DelayIsHonoured(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(2),
		mkq.WithBackoff(mkq.CustomBackoff()),
	)
	require.NoError(t, err)

	var mu sync.Mutex
	var attemptAt []time.Time

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		mu.Lock()
		attemptAt = append(attemptAt, time.Now())
		mu.Unlock()
		return nil, errors.New("nope")
	},
		mkq.WithIdlePollInterval(10*time.Millisecond),
		mkq.WithBackoffStrategyFunc(func(mkq.BackoffContext) time.Duration {
			return 400 * time.Millisecond
		}),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attemptAt, 2)
	gap := attemptAt[1].Sub(attemptAt[0])
	// 下限は寛容に (poll / スケジューリングの揺らぎを吸収)。即時再投入なら
	// 数ミリ秒で戻ってくるので、300ms 待てているかどうかで区別できる。
	assert.GreaterOrEqual(t, gap, 300*time.Millisecond, "retry should wait ~400ms, waited %v", gap)
}

// BullMQ's settings.backoffStrategy stops the retries by returning -1
// (job.ts: `delay == -1 ? false : true`). A strategy ported from
// TypeScript has to keep that meaning here, or "give up" would turn
// into "resend right now" and burn the remaining attempts back to back.
func TestWorker_BackoffContext_NegativeDelayStopsRetrying(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.CustomBackoff()),
	)
	require.NoError(t, err)

	var attempts atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		attempts.Add(1)
		return nil, errors.New("gone for good")
	},
		mkq.WithIdlePollInterval(10*time.Millisecond),
		mkq.WithBackoffStrategyFunc(func(mkq.BackoffContext) time.Duration {
			return -1 // BullMQ の -1 相当: これ以上は再試行しない
		}),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	// 残り 4 回ぶんが走っていないことを確かめる。走っていれば即時再投入なので
	// ここに到達する頃にはカウンタが上がっている。
	time.Sleep(200 * time.Millisecond)
	assert.EqualValues(t, 1, attempts.Load(), "a negative delay must stop the retries, not speed them up")

	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.Equal(t, "gone for good", h["failedReason"])
}

// A panicking strategy must not take the worker process down with it:
// the panic happens after runHandler's recover has returned, and the
// dispatch loop has no recover of its own.
func TestWorker_BackoffContext_PanicInStrategyDoesNotKillTheWorker(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(2),
		mkq.WithBackoff(mkq.CustomBackoff()),
	)
	require.NoError(t, err)

	var attempts atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		attempts.Add(1)
		return nil, errors.New("nope")
	},
		mkq.WithIdlePollInterval(10*time.Millisecond),
		mkq.WithBackoffStrategyFunc(func(mkq.BackoffContext) time.Duration {
			// **panic させるのがこのテストの目的。** strategy が落ちても
			// worker が死なないことを見ている。nil map への書き込みは
			// 「うっかり」で起きる代表例なので、わざとらしい panic() より
			// 現実の事故に近い。
			var m map[string]time.Time
			m["host"] = time.Now() //nolint:staticcheck // SA5000: 意図的
			return time.Second
		}),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	// panic は即時再試行にフォールバックする。ジョブが宙に浮かず、
	// 最後まで進むことを確かめる。
	assert.EqualValues(t, 2, attempts.Load())
}
