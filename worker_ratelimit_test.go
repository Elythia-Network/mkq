package mkq_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestWorker_RateLimit_Throttles pins that WithRateLimit(max, dur)
// caps dequeue throughput. We add 4 jobs with limit 2 / 200ms and
// expect total processing time ≥ ~200ms (one full window must elapse
// between the 2nd and 3rd job).
func TestWorker_RateLimit_Throttles(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const total = 4
	for i := range total {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}

	var done atomic.Int64
	start := time.Now()
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		done.Add(1)
		return nil, nil
	},
		mkq.WithRateLimit(2, 200*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(total)
	})

	elapsed := time.Since(start)
	// 4 jobs / 2 per 200ms = at minimum 1 cooldown window (~200ms).
	// Lower bound is generous to absorb CI jitter; upper bound checks
	// we didn't accidentally serialise everything (~800ms+).
	assert.GreaterOrEqual(t, elapsed, 180*time.Millisecond,
		"rate limit must enforce at least one cooldown window (got %v)", elapsed)
	assert.Less(t, elapsed, 800*time.Millisecond,
		"4 jobs with 2/200ms should fit in well under 800ms (got %v)", elapsed)
	assert.EqualValues(t, total, done.Load())
}

// TestWorker_RateLimit_DisabledByDefault regression-pins that
// omitting WithRateLimit (or passing zero values) keeps the
// pre-existing throughput.
func TestWorker_RateLimit_DisabledByDefault(t *testing.T) {
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

	start := time.Now()
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	},
		// No WithRateLimit — should run at full throughput.
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(total)
	})

	elapsed := time.Since(start)
	// Without a rate limiter 5 trivial jobs should finish very fast.
	assert.Less(t, elapsed, 500*time.Millisecond,
		"unlimited workers must process 5 trivial jobs quickly (got %v)", elapsed)
}

// TestWorker_RateLimit_ZeroDisablesLimiter verifies that
// WithRateLimit(0, 0) (or any non-positive arg) leaves the limiter
// disabled rather than crashing or producing infinite waits.
func TestWorker_RateLimit_ZeroDisablesLimiter(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	},
		mkq.WithRateLimit(0, 0), // no-op
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})
}
