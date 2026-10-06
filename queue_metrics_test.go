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

// TestQueueMetrics_Disabled_NoKeysWritten guards the default path:
// without WithJobMetrics, finalisation must not create the BullMQ
// metrics keys (matching pre-#59 behaviour, so existing deployments
// see no surprise).
func TestQueueMetrics_Disabled_NoKeysWritten(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-off")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 3
	for range n {
		_, err := queue.Add(ctx, testPayload{Body: "x"})
		require.NoError(t, err)
	}

	processed := make(chan struct{}, n)
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		processed <- struct{}{}
		return nil, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	for i := range n {
		select {
		case <-processed:
		case <-time.After(3 * time.Second):
			t.Fatalf("handler timeout after %d/%d", i, n)
		}
	}

	rdb := rawClient(t)
	base := prefix + ":metrics-off:metrics:"
	for _, kind := range []string{"completed", "failed"} {
		ex, _ := rdb.Exists(ctx, base+kind).Result()
		assert.EqualValues(t, 0, ex, "metrics:%s HASH must not exist when WithJobMetrics is unset", kind)
		ex, _ = rdb.Exists(ctx, base+kind+":data").Result()
		assert.EqualValues(t, 0, ex, "metrics:%s:data LIST must not exist when WithJobMetrics is unset", kind)
	}
}

// TestQueueMetrics_Enabled_CompletedFinalisation populates metrics
// via a worker with WithJobMetrics, then asserts the BullMQ-spec
// HASH count and the :data LIST head match. We pre-bump the worker's
// timestamp horizon by writing prevTS to (now-90s) so the lua adds
// at least one bucket on the first finalise.
func TestQueueMetrics_Enabled_CompletedFinalisation(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-on")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := rawClient(t)
	metricsKey := prefix + ":metrics-on:metrics:completed"
	dataKey := metricsKey + ":data"

	// Seed prevTS to (now - 90s). collectMetrics needs a non-nil
	// prevTS to advance N>=1; without seeding, the first finalise
	// only initialises prevTS without LPUSHing.
	pastMs := time.Now().Add(-90 * time.Second).UnixMilli()
	require.NoError(t, rdb.HSet(ctx, metricsKey, "prevTS", pastMs, "prevCount", 0).Err())

	const n = 4
	for i := range n {
		_, err := queue.Add(ctx, testPayload{Body: fmt.Sprintf("p%d", i)})
		require.NoError(t, err)
	}

	processed := make(chan struct{}, n)
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		processed <- struct{}{}
		return nil, nil
	}, mkq.WithJobMetrics(60))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	for i := range n {
		select {
		case <-processed:
		case <-time.After(5 * time.Second):
			t.Fatalf("handler timeout after %d/%d", i, n)
		}
	}

	// Wait for finalisation HMSET to settle (handler return is async
	// to moveToFinished).
	assert.Eventually(t, func() bool {
		count, _ := rdb.HGet(ctx, metricsKey, "count").Int64()
		return count == int64(n)
	}, 3*time.Second, 50*time.Millisecond, "metrics:completed count must reach %d", n)

	llen, _ := rdb.LLen(ctx, dataKey).Result()
	assert.GreaterOrEqual(t, llen, int64(1), ":data LIST must have at least one entry")
}

// TestQueueMetrics_Enabled_FailedFinalisation mirrors the completed
// path on the failed bucket. Handler always errors with attempts=1
// so the first invocation is the terminal failure.
func TestQueueMetrics_Enabled_FailedFinalisation(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-fail")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := rawClient(t)
	metricsKey := prefix + ":metrics-fail:metrics:failed"
	pastMs := time.Now().Add(-90 * time.Second).UnixMilli()
	require.NoError(t, rdb.HSet(ctx, metricsKey, "prevTS", pastMs, "prevCount", 0).Err())

	const n = 2
	for range n {
		_, err := queue.Add(ctx, testPayload{Body: "boom"}, mkq.WithAttempts(1))
		require.NoError(t, err)
	}

	var fired atomic.Int64
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		fired.Add(1)
		return nil, errors.New("intentional failure")
	}, mkq.WithJobMetrics(60))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	assert.Eventually(t, func() bool {
		count, _ := rdb.HGet(ctx, metricsKey, "count").Int64()
		return count == int64(n)
	}, 5*time.Second, 50*time.Millisecond, "metrics:failed count must reach %d", n)
}

// TestQueueMetrics_GetMetrics_ReadsBackBullMQShape asserts the
// public read API returns the meta + data + count triple in the
// same shape BullMQ TS's Queue.getMetrics returns. Populates via
// worker, then reads via Queue.GetMetrics.
func TestQueueMetrics_GetMetrics_ReadsBackBullMQShape(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-api")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := rawClient(t)
	metricsKey := prefix + ":metrics-api:metrics:completed"
	pastMs := time.Now().Add(-90 * time.Second).UnixMilli()
	require.NoError(t, rdb.HSet(ctx, metricsKey, "prevTS", pastMs, "prevCount", 0).Err())

	const n = 3
	for range n {
		_, err := queue.Add(ctx, testPayload{})
		require.NoError(t, err)
	}

	processed := make(chan struct{}, n)
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		processed <- struct{}{}
		return nil, nil
	}, mkq.WithJobMetrics(60))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	for range n {
		<-processed
	}

	assert.Eventually(t, func() bool {
		m, err := queue.GetMetrics(ctx, mkq.JobBucketCompleted, 0, -1)
		return err == nil && m.Meta.Count == int64(n)
	}, 3*time.Second, 50*time.Millisecond)

	got, err := queue.GetMetrics(ctx, mkq.JobBucketCompleted, 0, -1)
	require.NoError(t, err)
	assert.EqualValues(t, n, got.Meta.Count, "Meta.Count must reflect cumulative HASH count")
	assert.NotZero(t, got.Meta.PrevTS, "Meta.PrevTS must advance when the bucket rolls")
	assert.GreaterOrEqual(t, got.Count, int64(1), "Count (LLEN) >= 1 after one bucket roll")
	assert.NotEmpty(t, got.Data, "Data must contain at least one delta entry")
}

// TestQueueMetrics_GetMetrics_RejectsInvalidBucket guards the
// JobBucket validation: only completed / failed are valid.
func TestQueueMetrics_GetMetrics_RejectsInvalidBucket(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-bucket")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, bad := range []mkq.JobBucket{
		mkq.JobBucketWait,
		mkq.JobBucketActive,
		mkq.JobBucketDelayed,
		mkq.JobBucketPrioritized,
		mkq.JobBucketPaused,
	} {
		_, err := queue.GetMetrics(ctx, bad, 0, -1)
		assert.ErrorIs(t, err, mkq.ErrInvalidMetricsBucket, "bucket=%s", bad)
	}
}

// TestQueueMetrics_GetMetrics_RangeArgs verifies start / end forward
// to LRANGE correctly. Seed the data list directly with known
// values and read sub-ranges.
func TestQueueMetrics_GetMetrics_RangeArgs(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-range")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rdb := rawClient(t)
	dataKey := prefix + ":metrics-range:metrics:completed:data"
	// LPUSH each element -> "5","4","3","2","1" (newest first).
	for i := 1; i <= 5; i++ {
		require.NoError(t, rdb.LPush(ctx, dataKey, i).Err())
	}

	full, err := queue.GetMetrics(ctx, mkq.JobBucketCompleted, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, []int64{5, 4, 3, 2, 1}, full.Data, "full range, newest-first")
	assert.EqualValues(t, 5, full.Count)

	head2, err := queue.GetMetrics(ctx, mkq.JobBucketCompleted, 0, 1)
	require.NoError(t, err)
	assert.Equal(t, []int64{5, 4}, head2.Data, "head 2, newest-first")
}

// TestQueueMetrics_MultiBucket_PrevTSAdvance covers the lua's N>1
// branch (zero-padding for missed minutes). Set prevTS far into the
// past so collectMetrics computes N>1 on the next finalise.
func TestQueueMetrics_MultiBucket_PrevTSAdvance(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "metrics-multi")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := rawClient(t)
	metricsKey := prefix + ":metrics-multi:metrics:completed"
	dataKey := metricsKey + ":data"

	// 5 minutes ago: forces N=5 (capped by maxDataPoints below).
	pastMs := time.Now().Add(-5 * time.Minute).UnixMilli()
	require.NoError(t, rdb.HSet(ctx, metricsKey, "prevTS", pastMs, "prevCount", 0).Err())

	_, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	done := make(chan struct{}, 1)
	w, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		done <- struct{}{}
		return nil, nil
	}, mkq.WithJobMetrics(60))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	<-done

	assert.Eventually(t, func() bool {
		llen, _ := rdb.LLen(ctx, dataKey).Result()
		return llen >= 2
	}, 3*time.Second, 50*time.Millisecond, ":data must grow by >1 entry when prevTS lags by N minutes")
}
