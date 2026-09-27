package mkq_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestWorker_Delay_DoesNotConsumeAttempts pins the point of DelayedError:
// waiting must not eat the retry budget meant for real failures. With
// WithAttempts(2), three delays followed by failures must still give the
// handler two real attempts.
func TestWorker_Delay_DoesNotConsumeAttempts(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{},
		mkq.WithAttempts(2),
		mkq.WithBackoff(mkq.FixedBackoff(10*time.Millisecond)),
	)
	require.NoError(t, err)

	var runs, failures atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		if runs.Add(1) <= 3 {
			return nil, mkq.Delay(10 * time.Millisecond)
		}
		failures.Add(1)
		return nil, errors.New("real failure")
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})

	assert.EqualValues(t, 2, failures.Load(), "delays must leave both attempts for real failures")
	assert.EqualValues(t, 5, runs.Load())
	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.Equal(t, "2", h["atm"], "only real failures count as attempts")
}

// TestWorker_Delay_WaitsAndKeepsTheJob checks the job goes through the
// delayed set for at least the requested time, keeps its ID, and is not
// marked as failed while it waits.
func TestWorker_Delay_WaitsAndKeepsTheJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}, mkq.WithAttempts(3))
	require.NoError(t, err)

	const wait = 400 * time.Millisecond
	var mu sync.Mutex
	var seen []time.Time
	var ids []string
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		mu.Lock()
		seen = append(seen, time.Now())
		ids = append(ids, j.ID)
		n := len(seen)
		mu.Unlock()
		if n == 1 {
			// 包んでも効くこと (呼び出し側は理由を添えて返したい)。
			return nil, fmt.Errorf("downstream is down: %w", mkq.Delay(wait))
		}
		return "ok", nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// 待っている間は delayed にいて、失敗の痕跡を残さない。
	waitFor(t, ctx, 5*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"delayed", job.ID).Result()
		return v > 0
	})
	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.Equal(t, "", h["failedReason"], "a delay is not a failure")
	// 一度も失敗していなければ atm は未設定のまま (BullMQ も失敗時にだけ HINCRBY する)。
	assert.Contains(t, []string{"", "0"}, h["atm"], "a delay must not bump atm")

	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 2)
	assert.GreaterOrEqual(t, seen[1].Sub(seen[0]), wait-20*time.Millisecond, "must not come back before the delay")
	assert.Equal(t, ids[0], ids[1], "the same job comes back")
}

// A delay is not reported as a failure: no failed counter, no span error.
// (The CHANGELOG promises this; without a test, reporting it as failed
// would pass every other check.)
func TestWorker_Delay_NotCountedAsFailure(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c, _, metrics, tracer := newClientWithObservers(t, prefix)
	queue := mkq.Define[testPayload](c, "delaymetrics")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	var runs atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		if runs.Add(1) == 1 {
			return nil, mkq.Delay(10 * time.Millisecond)
		}
		return "ok", nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	status := func(s string) float64 {
		return metrics.counter(mkq.MetricJobsProcessedTotal,
			slog.String(mkq.AttrQueue, "delaymetrics"),
			slog.String(mkq.AttrJobName, "delaymetrics"),
			slog.String(mkq.AttrProcessStatus, s),
		)
	}
	assert.Eventually(t, func() bool { return status("completed") == 1 }, 5*time.Second, 20*time.Millisecond)
	assert.Zero(t, status("failed"), "a delay must not count as a failed job")
	for _, sp := range tracer.snapshot() {
		sp.mu.Lock()
		spanErr := sp.err
		sp.mu.Unlock()
		assert.NoError(t, spanErr, "a delay must not mark the span as errored")
	}
}

// A non-positive delay still goes through delayed instead of failing the
// job or spinning.
func TestWorker_Delay_NonPositiveStillRequeues(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// attempts を付けない (= 1 回きり)。遅延が失敗扱いなら、ここで failed に落ちる。
	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	var runs atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		if runs.Add(1) == 1 {
			return nil, mkq.Delay(-time.Second)
		}
		return "ok", nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})
	assert.EqualValues(t, 2, runs.Load())
}

func TestDelayedError_Unwraps(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("wrapped: %w", mkq.Delay(3*time.Second))
	var de *mkq.DelayedError
	require.True(t, errors.As(err, &de))
	assert.Equal(t, 3*time.Second, de.Delay)
	assert.False(t, errors.Is(err, mkq.ErrUnrecoverable))
}

// Wrapping both puts the job back rather than failing it (documented on
// DelayedError).
func TestWorker_Delay_WinsOverUnrecoverable(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}, mkq.WithAttempts(3))
	require.NoError(t, err)

	var runs atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		if runs.Add(1) == 1 {
			return nil, errors.Join(mkq.Delay(10*time.Millisecond), mkq.ErrUnrecoverable)
		}
		return "ok", nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})
	assert.EqualValues(t, 2, runs.Load())
}
