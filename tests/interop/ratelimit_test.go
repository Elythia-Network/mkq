//go:build interop

// Cross-language rate limiter: an mkq Worker and a BullMQ TS Worker
// process the same queue concurrently with the same limiter config
// (max=2 jobs / 500ms). They share the limiter:<groupKey> ZSET
// state via the vendored Lua's getRateLimitTTL, so the combined
// throughput must respect the limit regardless of which worker pulls.
//
// Pre-enqueue 6 jobs, run both workers concurrently, assert total
// wall time stays >= ~1s (= 3 cooldown windows). If the limiter
// state weren't shared, both workers would drain in <300ms.

package interop_test

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

func TestInterop_RateLimitShared(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "rl"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const total = 6
	for i := range total {
		_, err := queue.Add(ctx, interopPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}

	// BullMQ TS Worker with limiter — it'll INCR rateLimiterKey and
	// observe the cooldown via getRateLimitTTL.
	startNodeWorkerOpts(t, prefix, queueName, nodeWorkerOpts{
		Concurrency:       1,
		LimiterMax:        2,
		LimiterDurationMs: 500,
	})

	// mkq Worker with the same limiter config.
	var mkqDone atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[interopPayload]) (any, error) {
		mkqDone.Add(1)
		return nil, nil
	},
		mkq.WithConcurrency(1),
		mkq.WithRateLimit(2, 500*time.Millisecond),
		mkq.WithIdlePollInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	start := time.Now()
	waitFor(t, ctx, 100*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n == int64(total)
	})
	elapsed := time.Since(start)

	// 6 jobs / 2 per 500ms = 3 windows. Allow generous lower bound
	// for CI jitter; the strict invariant is "noticeably slower than
	// unlimited" — without the shared limiter, two concurrent workers
	// drain the queue in well under 500ms.
	assert.GreaterOrEqual(t, elapsed, 700*time.Millisecond,
		"shared rate limit must throttle the combined throughput (got %v)", elapsed)
	// Both workers should have contributed, so mkq processed > 0 jobs.
	assert.Greater(t, mkqDone.Load(), int64(0),
		"mkq Worker should have processed at least one job (got %d)", mkqDone.Load())
}
