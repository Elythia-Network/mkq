package mkq_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestWorker_Stalled_RecoversFromDeadWorker simulates a worker that
// acquires a job and never heartbeats (lockDuration is shorter than
// the handler's "work"). A second worker, with stalled detection
// fast-cycling, must reclaim the job and run it to completion on
// the live worker.
func TestWorker_Stalled_RecoversFromDeadWorker(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "stalled"})
	require.NoError(t, err)

	// "Dead" worker simulation: the handler enters but does NOT
	// watch ctx, so the dispatch loop stays blocked inside the
	// handler. This prevents the dead worker from re-acquiring the
	// job after stalled detection moves it back to wait — the
	// realistic equivalent of a worker process that's been killed
	// or has lost its Redis connection. Sub-second lockDuration
	// guarantees the lock TTL expires before the heartbeat clamp
	// (>= 1s) can renew it.
	deadHandlerEntered := make(chan struct{}, 1)
	deadHandlerRelease := make(chan struct{})
	deadWorker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		select {
		case deadHandlerEntered <- struct{}{}:
		default:
		}
		<-deadHandlerRelease
		return nil, nil
	},
		mkq.WithLockDuration(400*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		// Disable stalled detection on the dead worker so it can't
		// reclaim its own job.
		mkq.WithStalledInterval(0),
	)
	require.NoError(t, err)
	defer func() {
		// Release the dead handler so Worker.Stop can drain. Use a
		// bounded ctx so Stop never hangs the test.
		close(deadHandlerRelease)
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = deadWorker.Stop(stopCtx)
	}()

	// Wait until the dead worker has the job in active.
	receiveOrFail(t, ctx, deadHandlerEntered)

	// Live worker: short stalled interval so the test doesn't drag.
	var liveRan atomic.Int64
	liveWorker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		liveRan.Add(1)
		return nil, nil
	},
		mkq.WithLockDuration(5*time.Second),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithStalledInterval(300*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, liveWorker)

	// Stalled detection in BullMQ requires two ticks: the first
	// adds the dead worker's active job to the stalled SET, the
	// second observes the missing lock and re-enqueues. Plus the
	// 400ms lockDuration must elapse. Allow a generous wait.
	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 100*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	assert.GreaterOrEqual(t, liveRan.Load(), int64(1), "live worker must run the recovered job")
}

// TestWorker_Stalled_FailsAfterMaxStalledCount pins BullMQ's
// "stalled too many times" failure path: when a job is reclaimed
// more than maxStalledCount times the Lua writes the `defa` HASH
// field with BullMQ's canonical message.
func TestWorker_Stalled_FailsAfterMaxStalledCount(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	// Workers that grab the job, hold it past the lock TTL, then
	// release ctx (so the test eventually exits).
	hold := func(ctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	// Use very short lockDuration so each "stall" finishes quickly.
	// Two workers with stalled detection so they keep re-acquiring
	// each other's expired-lock job.
	wA, err := mkq.Process(queue, hold,
		mkq.WithLockDuration(500*time.Millisecond),
		mkq.WithStalledInterval(300*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithMaxStalledCount(1),
	)
	require.NoError(t, err)
	wB, err := mkq.Process(queue, hold,
		mkq.WithLockDuration(500*time.Millisecond),
		mkq.WithStalledInterval(300*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
		mkq.WithMaxStalledCount(1),
	)
	require.NoError(t, err)
	defer stopWorker(t, wA)
	defer stopWorker(t, wB)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Final state assertion: a worker re-acquires the stalled-too-many
	// job via moveToActive, observes BullMQ's `defa` HASH field, and
	// finalises straight to failed (skipping the handler). The
	// failed-branch Lua then HDELs `defa`, so by the time we observe
	// the failed ZSET membership, only `failedReason` survives. We
	// don't try to catch the transient `defa` snapshot — the worker's
	// dequeue loop is too fast to make that observable race-free.
	waitFor(t, ctx, 100*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", job.ID).Result()
		return v > 0
	})
	failedReason, _ := rdb.HGet(ctx, base+job.ID, "failedReason").Result()
	assert.Equal(t, "job stalled more than allowable limit", failedReason,
		"defa must be persisted as failedReason on the terminal failed state")
	finalDefa, _ := rdb.HGet(ctx, base+job.ID, "defa").Result()
	assert.Empty(t, finalDefa, "moveToFinished failed must HDEL defa")
}

// TestWorker_Stalled_HealthyWorkerNeverStalls regression-pins that a
// normally heartbeating worker is not flagged as stalled even with
// aggressive stalled detection.
func TestWorker_Stalled_HealthyWorkerNeverStalls(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	// Handler does real work briefly but well within lockDuration;
	// stalled detection cycles many times during that window.
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return nil, nil
	},
		mkq.WithLockDuration(5*time.Second),
		mkq.WithStalledInterval(50*time.Millisecond),
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	// stc must NOT be incremented for a healthy worker. go-redis v9
	// returns redis.Nil when the field is absent, which is the
	// "stc was never written" case we're asserting.
	stc, err := rdb.HGet(ctx, base+job.ID, "stc").Result()
	if err == redis.Nil {
		stc = ""
	} else {
		require.NoError(t, err)
	}
	assert.True(t, stc == "" || stc == "0", "healthy worker must not bump stc, got %q", stc)
}

// TestWorker_Stalled_RepeatableJobIsNotHardFailed pins the one genuinely
// new piece of wiring in the BullMQ 6 migration: moveStalledJobsToWait
// gained KEYS[9] (the `repeat` key) so it can tell a scheduler-owned job
// from an ordinary one.
//
// **BullMQ 6 で判定方法が変わった。** v5 は job HASH の `opts` を JSON
// デコードして `repeat` があるかを見ていた。6 は `rjk` フィールドから
// scheduler id を読み、`{repeat key}:{id}` の HASH が実在するかを確かめる。
// つまり KEYS[9] に間違った key を渡すと判定が静かに false に倒れ、
// 周期ジョブが stall 回数超過で恒久 fail する。Go 側が組み立てる KEYS は
// ここでしか効かないので、専用のテストが無いと取り違えに気付けない。
func TestWorker_Stalled_RepeatableJobIsNotHardFailed(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, "ticker", 100*time.Millisecond, testPayload{Inbox: "tick"}))

	// ハンドラは返らない。周期ジョブは「完了してから次を積む」ので、
	// インスタンスは最初の 1 件だけで増えない。
	hold := func(ctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	opts := []mkq.WorkerOption{
		mkq.WithLockDuration(500 * time.Millisecond),
		mkq.WithStalledInterval(300 * time.Millisecond),
		mkq.WithIdlePollInterval(20 * time.Millisecond),
		mkq.WithMaxStalledCount(1),
	}
	wA, err := mkq.Process(queue, hold, opts...)
	require.NoError(t, err)
	wB, err := mkq.Process(queue, hold, opts...)
	require.NoError(t, err)
	defer stopWorker(t, wA)
	defer stopWorker(t, wB)

	rdb := rawClient(t)
	base := prefix + ":tick:"

	// scheduler HASH が Lua の見に行く場所にあること。ここがずれていたら
	// 以降の assert は「たまたま fail しなかった」だけになる。
	exists, err := rdb.Exists(ctx, base+"repeat:ticker").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, exists, "the scheduler HASH must live at {queue}:repeat:{id}")

	// stc が maxStalledCount(1) を超えるまで待つ。超えて初めて
	// 「fail するかしないか」の分岐に入るので、ここを待たないテストは
	// 空虚になる。
	var jobID string
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		ids, err := rdb.LRange(ctx, base+"active", 0, -1).Result()
		if err != nil || len(ids) == 0 {
			return false
		}
		jobID = ids[0]
		return true
	})
	require.NotEmpty(t, jobID)

	// stc は job HASH に残るので、取り違えで恒久 fail した場合でも
	// active から消えたせいで待ち続ける、ということにはならない。
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		stc, _ := rdb.HGet(ctx, base+jobID, "stc").Int64()
		return stc > 1
	})
	assert.True(t, strings.HasPrefix(jobID, "repeat:ticker:"), "unexpected job id %q", jobID)

	rjk, err := rdb.HGet(ctx, base+jobID, "rjk").Result()
	require.NoError(t, err)
	require.Equal(t, "ticker", rjk, "the scheduler id must be stamped on the job as rjk")

	// 周期ジョブは stall 回数超過でも恒久 fail しない。
	defa, _ := rdb.HGet(ctx, base+jobID, "defa").Result()
	assert.Empty(t, defa, "a scheduler-owned job must not be marked deferred-failed for stalling")

	failedScore, _ := rdb.ZScore(ctx, base+"failed", jobID).Result()
	assert.Zero(t, failedScore, "a scheduler-owned job must not be moved to failed for stalling")
}
