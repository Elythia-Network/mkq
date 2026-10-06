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

// retryAfterErr is the shape a caller would realistically use: a typed
// error carrying the server's hint, pulled out with errors.As.
type retryAfterErr struct {
	after time.Duration
}

func (e *retryAfterErr) Error() string { return "rate limited, retry after " + e.after.String() }

// delayedScoreOf returns how far in the future the job's delayed-ZSET
// entry sits, which is the only place the computed delay is observable.
//
// **スコアは生のミリ秒ではない。** BullMQ は FIFO を保つために timestamp を
// 12 bit 左シフトし、下位に job の連番を詰める (`getDelayedScore.lua`)。
// 割り戻さないと 292 年後になる。
func delayedScoreOf(t *testing.T, ctx context.Context, prefix, queue, jobID string) time.Duration {
	t.Helper()
	score, err := rawClient(t).ZScore(ctx, prefix+":"+queue+":delayed", jobID).Result()
	require.NoError(t, err, "the job must be sitting in delayed")
	return time.Until(time.UnixMilli(int64(score) / 0x1000))
}

// runUntilDelayed fails a job once through a worker and returns as soon
// as it lands in the delayed ZSET.
func runUntilDelayed(
	t *testing.T, ctx context.Context, prefix string, q *mkq.Queue[testPayload],
	jobID string, handlerErr error, opts ...mkq.WorkerOption,
) {
	t.Helper()
	var failed atomic.Int64
	base := []mkq.WorkerOption{
		mkq.WithConcurrency(1),
		mkq.WithIdlePollInterval(20 * time.Millisecond),
	}
	worker, err := mkq.Process(q, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		failed.Add(1)
		return nil, handlerErr
	}, append(base, opts...)...)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		if failed.Load() == 0 {
			return false
		}
		n, err := rawClient(t).ZScore(ctx, prefix+":deliver:delayed", jobID).Result()
		return err == nil && n > 0
	})
}

// TestRetryDelayOverride_AppliesToBuiltinBackoff is the case the hook
// exists for.
//
// **exponential のキューのまま、この失敗だけ別の遅延にしたい。**
// `WithBackoffStrategyFunc` は backoff type が custom のときしか呼ばれない
// ので、これを書くには custom に切り替えてカーブ全体を持つしかなかった。
func TestRetryDelayOverride_AppliesToBuiltinBackoff(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.BackoffStrategy{Type: "exponential", Delay: time.Second}),
	)
	require.NoError(t, err)

	var seen atomic.Int64
	runUntilDelayed(t, ctx, prefix, queue, job.ID, &retryAfterErr{after: 90 * time.Second},
		mkq.WithRetryDelayOverride(func(bc mkq.BackoffContext) (time.Duration, bool) {
			seen.Add(1)
			var ra *retryAfterErr
			if errors.As(bc.Err, &ra) {
				return ra.after, true
			}
			return 0, false
		}))

	require.Positive(t, seen.Load(), "the override must be consulted for exponential jobs")
	got := delayedScoreOf(t, ctx, prefix, "deliver", job.ID)
	assert.InDelta(t, (90 * time.Second).Seconds(), got.Seconds(), 5,
		"the override's delay wins over the exponential curve (which would be ~1s)")

	// opts は書き換えない。別言語の worker が同じキューを読んだときに
	// 自分の backoff で動けること。
	raw, err := rawClient(t).HGet(ctx, prefix+":deliver:"+job.ID, "opts").Result()
	require.NoError(t, err)
	assert.Contains(t, raw, `"type":"exponential"`,
		"the override must not rewrite the job's backoff on the wire")
	assert.NotContains(t, raw, "custom")
}

// Declining must land exactly where the configured backoff would have.
func TestRetryDelayOverride_DecliningUsesConfiguredBackoff(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.BackoffStrategy{Type: "fixed", Delay: 30 * time.Second}),
	)
	require.NoError(t, err)

	var seen atomic.Int64
	runUntilDelayed(t, ctx, prefix, queue, job.ID, errors.New("boom"),
		mkq.WithRetryDelayOverride(func(mkq.BackoffContext) (time.Duration, bool) {
			seen.Add(1)
			return 12 * time.Hour, false // 値は返すが ok=false なので無視される
		}))

	require.Positive(t, seen.Load())
	got := delayedScoreOf(t, ctx, prefix, "deliver", job.ID)
	assert.InDelta(t, (30 * time.Second).Seconds(), got.Seconds(), 5,
		"declining must fall through to the fixed 30s, not the declined value")
}

// A job with no backoff at all still gets asked. Without this the hook
// would be useless for queues that never configured one.
func TestRetryDelayOverride_AppliesWhenJobHasNoBackoff(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithAttempts(5))
	require.NoError(t, err)

	runUntilDelayed(t, ctx, prefix, queue, job.ID, errors.New("boom"),
		mkq.WithRetryDelayOverride(func(mkq.BackoffContext) (time.Duration, bool) {
			return 45 * time.Second, true
		}))

	got := delayedScoreOf(t, ctx, prefix, "deliver", job.ID)
	assert.InDelta(t, (45 * time.Second).Seconds(), got.Seconds(), 5,
		"a job without backoff would otherwise retry immediately")
}

// A negative duration gives up, matching what a custom strategy does.
func TestRetryDelayOverride_NegativeFailsTheJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithAttempts(5))
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("boom")
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithRetryDelayOverride(func(mkq.BackoffContext) (time.Duration, bool) {
			return -1, true
		}))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		counts, err := queue.Counts(ctx, mkq.JobBucketFailed)
		return err == nil && counts.Failed == 1
	})

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, counts.Failed, "a negative delay stops the retries")
	assert.Zero(t, counts.Delayed)
}

// The hook runs outside runHandler's recover, so a panic there would
// otherwise take the dispatch goroutine — and the job's lock — with it.
func TestRetryDelayOverride_PanicIsContained(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.BackoffStrategy{Type: "fixed", Delay: 30 * time.Second}),
	)
	require.NoError(t, err)

	runUntilDelayed(t, ctx, prefix, queue, job.ID, errors.New("boom"),
		mkq.WithRetryDelayOverride(func(mkq.BackoffContext) (time.Duration, bool) {
			panic("override blew up")
		}))

	// **panic は「即時再試行」ではなく「意見なし」に落ちる。** 0 に落とすと、
	// 熟慮された backoff が黙ってホットループに変わる。
	got := delayedScoreOf(t, ctx, prefix, "deliver", job.ID)
	assert.InDelta(t, (30 * time.Second).Seconds(), got.Seconds(), 5,
		"a panicking override falls back to the configured backoff, not to delay 0")
}

// The context handed to the hook must carry what a decision needs.
func TestRetryDelayOverride_ReceivesJobContext(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(5),
		mkq.WithJobName("deliver-one"),
		mkq.WithBackoff(mkq.BackoffStrategy{Type: "exponential", Delay: time.Second}),
	)
	require.NoError(t, err)

	var got atomic.Value
	runUntilDelayed(t, ctx, prefix, queue, job.ID, fmt.Errorf("wrapped: %w", &retryAfterErr{after: time.Minute}),
		mkq.WithRetryDelayOverride(func(bc mkq.BackoffContext) (time.Duration, bool) {
			got.Store(bc)
			return time.Minute, true
		}))

	bc, ok := got.Load().(mkq.BackoffContext)
	require.True(t, ok, "the override was never called")
	assert.Equal(t, job.ID, bc.JobID)
	assert.Equal(t, "deliver-one", bc.Name)
	assert.EqualValues(t, 1, bc.AttemptsMade)
	assert.Equal(t, "exponential", bc.BackoffType,
		"the configured type is visible even though the override runs for every type")

	var ra *retryAfterErr
	require.True(t, errors.As(bc.Err, &ra), "the handler error must survive wrapping")
	assert.Equal(t, time.Minute, ra.after)
}

// Without an override registered nothing changes.
func TestRetryDelayOverride_AbsentLeavesBackoffAlone(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(5),
		mkq.WithBackoff(mkq.BackoffStrategy{Type: "fixed", Delay: 25 * time.Second}),
	)
	require.NoError(t, err)

	runUntilDelayed(t, ctx, prefix, queue, job.ID, errors.New("boom"))

	got := delayedScoreOf(t, ctx, prefix, "deliver", job.ID)
	assert.InDelta(t, (25 * time.Second).Seconds(), got.Seconds(), 5)
}

// The override decides how long to wait, not whether to wait at all.
//
// **試行回数を伸ばす手段ではない。** 再試行するかどうかは WithAttempts と
// ErrUnrecoverable が先に決めていて、そこで打ち切られたジョブには呼ばれない。
// 呼ばれてしまうと「あと 1 回だけ」のつもりの設定が無視されることになる。
func TestRetryDelayOverride_NotConsultedOnceAttemptsAreSpent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// attempts=1 なので最初の失敗で打ち切り。
	_, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithAttempts(1))
	require.NoError(t, err)

	var consulted atomic.Int64
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("boom")
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithRetryDelayOverride(func(mkq.BackoffContext) (time.Duration, bool) {
			consulted.Add(1)
			return time.Hour, true
		}))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		counts, err := queue.Counts(ctx, mkq.JobBucketFailed)
		return err == nil && counts.Failed == 1
	})

	assert.Zero(t, consulted.Load(),
		"a job past its attempt budget must not reach the override")

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.Zero(t, counts.Delayed, "the override could not have rescued it into delayed")
}

// ErrUnrecoverable も同じ。handler が「もう試すな」と言っているのに
// override が遅延を返して生き延びる、ということが無いこと。
func TestRetryDelayOverride_NotConsultedOnUnrecoverable(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithAttempts(5))
	require.NoError(t, err)

	var consulted atomic.Int64
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, fmt.Errorf("gone: %w", mkq.ErrUnrecoverable)
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithRetryDelayOverride(func(mkq.BackoffContext) (time.Duration, bool) {
			consulted.Add(1)
			return time.Hour, true
		}))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		counts, err := queue.Counts(ctx, mkq.JobBucketFailed)
		return err == nil && counts.Failed == 1
	})

	assert.Zero(t, consulted.Load(),
		"ErrUnrecoverable must not reach the override")
}
