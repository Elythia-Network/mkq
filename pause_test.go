package mkq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestQueue_Pause_LeavesJobsInWait verifies the BullMQ 6 pause
// semantics: the meta.paused flag is set, nothing is relocated, and
// Counts / ListJobs / IsPaused all agree on what is held back.
func TestQueue_Pause_LeavesJobsInWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ids []string
	for range 3 {
		job, err := queue.Add(ctx, testPayload{Inbox: "to-park"})
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}

	require.NoError(t, queue.Pause(ctx))

	paused, err := queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.True(t, paused, "IsPaused must report true after Pause")

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// **BullMQ 6 で pause はジョブを動かさない。** v5 は wait を paused へ
	// RENAME して退避していたが、6 では wait に残したまま meta.paused の
	// フラグだけで止める。退避が無いので pause のコストがキューの長さに
	// 依存しない。
	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, ids, waitMembers, "jobs must stay in wait")

	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "the legacy paused list must not be used")

	flag, err := rdb.HGet(ctx, base+"meta", "paused").Result()
	require.NoError(t, err)
	assert.Equal(t, "1", flag, "meta.paused must be set to 1")

	// Pause must DEL the marker (BullMQ pause-7.lua) so idle blocking
	// workers stop being woken to spin against the gate.
	markerCard, err := rdb.ZCard(ctx, base+"marker").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, markerCard, "marker must be deleted by Pause")

	// Counts.Paused は「停止中に溜まっている件数」を答える。ジョブは wait に
	// あるので、同じ 3 件が Wait にも数えられる。
	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, counts.Paused, "Counts.Paused must report what is waiting while paused")
	assert.EqualValues(t, 3, counts.Wait, "the jobs are in wait, so Wait counts them too")

	// 一覧も同じものを返さないと、件数と中身が食い違う。
	listed, err := queue.ListJobs(ctx, mkq.JobBucketPaused, 0, -1, true)
	require.NoError(t, err)
	var listedIDs []string
	for _, j := range listed {
		listedIDs = append(listedIDs, j.Job.ID)
	}
	assert.ElementsMatch(t, ids, listedIDs, "ListJobs(paused) must agree with Counts.Paused")
}

// TestQueue_Resume_MovesPausedBackToWait verifies that Resume clears the
// flag, leaves the jobs available in wait, and pokes the marker ZSET so
// a blocking worker wakes immediately.
func TestQueue_Resume_MovesPausedBackToWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ids []string
	for range 3 {
		job, err := queue.Add(ctx, testPayload{Inbox: "to-resume"})
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}
	require.NoError(t, queue.Pause(ctx))
	require.NoError(t, queue.Resume(ctx))

	paused, err := queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.False(t, paused, "IsPaused must report false after Resume")

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "paused list must be empty after Resume")

	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, ids, waitMembers, "all jobs must be back in wait")

	exists, err := rdb.HExists(ctx, base+"meta", "paused").Result()
	require.NoError(t, err)
	assert.False(t, exists, "meta.paused must be removed after Resume")

	// Resume must add the base marker so blocking workers wake up.
	markerScore, err := rdb.ZScore(ctx, base+"marker", "0").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, markerScore, "marker must hold the base entry after Resume")
}

// TestQueue_Add_DuringPause_GoesToWait is the orphan-safety acceptance
// criterion under BullMQ 6: a job enqueued while the queue is paused
// lands in wait like any other, and stays put across Resume — the gate,
// not the job's location, is what held it back.
func TestQueue_Add_DuringPause_GoesToWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	job, err := queue.Add(ctx, testPayload{Inbox: "enqueued-while-paused"})
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// **BullMQ 6 は pause 中でも wait に入れる。** v5 は getTargetQueueList で
	// paused へ振り分けていたが、その関数ごと消えた。止めるのは worker 側の
	// gate (meta.paused) で、置き場所は変えない。
	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{job.ID}, waitMembers, "a job added during pause goes to wait")

	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "the legacy paused list must not be used")

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, counts.Paused, "Counts.Paused must include the job added during pause")

	require.NoError(t, queue.Resume(ctx))

	// 移動が無いので、resume してもジョブは動かない。止まっていた gate が
	// 開くだけ。
	waitMembers, err = rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{job.ID}, waitMembers, "Resume does not move the job; it lifts the gate")

	counts, err = queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, counts.Paused, "Counts.Paused is zero once the queue is running")
}

// TestQueue_PauseResume_Idempotent confirms pause-7.lua's no-op
// semantics: pausing an already-paused queue (or resuming an unpaused
// one) is harmless and never errors.
func TestQueue_PauseResume_Idempotent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Resume on a never-paused queue is a no-op.
	require.NoError(t, queue.Resume(ctx))
	paused, err := queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.False(t, paused)

	require.NoError(t, queue.Pause(ctx))
	require.NoError(t, queue.Pause(ctx), "double Pause must not error")
	paused, err = queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.True(t, paused)

	require.NoError(t, queue.Resume(ctx))
	require.NoError(t, queue.Resume(ctx), "double Resume must not error")
	paused, err = queue.IsPaused(ctx)
	require.NoError(t, err)
	assert.False(t, paused)
}

// TestQueue_Pause_WorkerDoesNotFetch verifies the end-to-end gate: a job
// parked while paused is not handed to a worker until Resume. The same
// worker that idles through the pause must pick the job up promptly once
// the queue resumes (marker poke).
func TestQueue_Pause_WorkerDoesNotFetch(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	job, err := queue.Add(ctx, testPayload{Inbox: "gated"})
	require.NoError(t, err)

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	// The worker must NOT fetch the parked job while paused.
	select {
	case id := <-processed:
		t.Fatalf("worker processed job %s while queue was paused", id)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, queue.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "worker must process the job after Resume")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the job after Resume")
	}
}

// TestQueue_Pause_PrioritizedNotFetched extends the pause gate to the
// prioritized bucket. The moveToActive gate returns before the
// prioritized->active move, so a prioritized job must not be dispatched
// while paused — and it stays in the prioritized ZSET (Pause does not
// rename prioritized into paused), so Counts.Paused excludes it.
func TestQueue_Pause_PrioritizedNotFetched(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	job, err := queue.Add(ctx, testPayload{Inbox: "prio-gated"}, mkq.WithPriority(5))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Prioritized job lands in the prioritized ZSET, not the paused list.
	prioCard, err := rdb.ZCard(ctx, base+"prioritized").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, prioCard, "prioritized job must stay in the prioritized ZSET")
	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, counts.Prioritized)
	assert.EqualValues(t, 0, counts.Paused, "prioritized job is not counted as paused")

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	// The prioritized job must NOT be fetched while paused.
	select {
	case id := <-processed:
		t.Fatalf("worker processed prioritized job %s while paused", id)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, queue.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "prioritized job must process after Resume")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the prioritized job after Resume")
	}
}

// TestQueue_Pause_DelayedMaturesToPaused is the orphan-safety criterion
// for delayed jobs. A delayed job whose timer fires while the queue is
// paused must be promoted into the paused list (not wait), so it is not
// dispatched until Resume. promoteDelayedJobs runs inside moveToActive
// (driven by the idle worker) and honours the pause target, so the
// matured job is parked, not handed out.
func TestQueue_Pause_DelayedMaturesButIsNotProcessed(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))

	processed := make(chan string, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	job, err := queue.Add(ctx, testPayload{Inbox: "delayed-gated"}, mkq.WithDelay(200*time.Millisecond))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// Wait until the idle worker's moveToActive has promoted the matured
	// delayed job out of the delayed ZSET.
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"delayed").Result()
		return n == 0
	})

	// 満期になった delayed job は wait に入る。BullMQ 6 では pause 中でも
	// 置き場所は変わらないので、「処理されないこと」を gate 側で確かめる。
	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.Contains(t, waitMembers, job.ID, "a matured delayed job lands in wait")
	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "the legacy paused list must not be used")

	select {
	case id := <-processed:
		t.Fatalf("worker processed delayed job %s while paused", id)
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, queue.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "delayed job must process after Resume")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the delayed job after Resume")
	}
}

// TestQueue_Pause_ClusterHonorAcrossClients verifies the multi-process
// acceptance criterion: a pause issued through one Client is honoured by
// a worker created from a different Client against the same Redis. The
// gate lives in moveToActive's Lua (shared meta.paused), so there is no
// client-local pause state for a second process to miss.
func TestQueue_Pause_ClusterHonorAcrossClients(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	cAdmin := newClient(t, prefix)
	cWorker := newClient(t, prefix)

	queueAdmin := mkq.Define[testPayload](cAdmin, "deliver")
	queueWorker := mkq.Define[testPayload](cWorker, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Pause through the admin Client before the worker Client enqueues.
	require.NoError(t, queueAdmin.Pause(ctx))

	job, err := queueWorker.Add(ctx, testPayload{Inbox: "cluster-gated"})
	require.NoError(t, err)

	processed := make(chan string, 1)
	worker, err := mkq.Process(queueWorker, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		processed <- j.ID
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, worker.Stop(stopCtx))
	}()

	// The worker on cWorker must honour the pause set by cAdmin.
	select {
	case id := <-processed:
		t.Fatalf("worker (cWorker) processed job %s despite pause set by cAdmin", id)
	case <-time.After(500 * time.Millisecond):
	}

	// Resume through the admin Client; the worker Client must pick up.
	require.NoError(t, queueAdmin.Resume(ctx))

	select {
	case id := <-processed:
		assert.Equal(t, job.ID, id, "worker must process after admin Client resumes")
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process after cross-Client Resume")
	}
}

// TestQueue_PauseResume_EmitsEvents verifies the BullMQ QueueEvents wire
// contract: Pause / Resume each XADD a "paused" / "resumed" event onto
// the events stream. mkq has no dedicated event struct for these yet, so
// they surface as RawEvent (forward-compatible), which is what
// bull-board / Misskey QueueEvents consumers read.
func TestQueue_PauseResume_EmitsEvents(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	qe := mkq.NewQueueEvents(queue)
	events, _ := startSubscriber(t, qe)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.Pause(ctx))
	require.NoError(t, queue.Resume(ctx))

	tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tcancel()
	got := drainUntil(t, tctx, events, func(xs []mkq.Event) bool {
		var sawPaused, sawResumed bool
		for _, ev := range xs {
			if raw, ok := ev.(mkq.RawEvent); ok {
				switch raw.Type {
				case "paused":
					sawPaused = true
				case "resumed":
					sawResumed = true
				}
			}
		}
		return sawPaused && sawResumed
	})

	var types []string
	for _, ev := range got {
		if raw, ok := ev.(mkq.RawEvent); ok {
			types = append(types, raw.Type)
		}
	}
	assert.Contains(t, types, "paused", "Pause must XADD a paused event")
	assert.Contains(t, types, "resumed", "Resume must XADD a resumed event")
}

// simulateLegacyPause puts the queue into the state a BullMQ 5 writer
// would leave behind: the meta.paused flag set and the waiting jobs
// parked in the separate `paused` LIST. BullMQ 6 never writes that list,
// so it can only be produced by hand here.
func simulateLegacyPause(t *testing.T, ctx context.Context, prefix string) {
	t.Helper()
	rdb := rawClient(t)
	base := prefix + ":deliver:"
	require.NoError(t, rdb.HSet(ctx, base+"meta", "paused", 1).Err())
	if n, err := rdb.Exists(ctx, base+"wait").Result(); err == nil && n == 1 {
		require.NoError(t, rdb.Rename(ctx, base+"wait", base+"paused").Err())
	}
}

// TestQueue_Counts_ReportsLegacyPausedList covers the v5 -> v6 window: a
// queue paused by a BullMQ 5 writer still holds its backlog in the
// legacy `paused` list. Counts must report those jobs rather than the
// (now empty) wait list, and ListJobs must return the same set — if the
// two disagree an admin UI shows "5 paused" over an empty table.
func TestQueue_Counts_ReportsLegacyPausedList(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ids []string
	for range 3 {
		job, err := queue.Add(ctx, testPayload{Inbox: "legacy"})
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}
	simulateLegacyPause(t, ctx, prefix)

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, counts.Paused, "the legacy paused list must be reported as paused")
	assert.EqualValues(t, 0, counts.Wait, "wait is empty in the v5 layout")

	listed, err := queue.ListJobs(ctx, mkq.JobBucketPaused, 0, -1, true)
	require.NoError(t, err)
	var listedIDs []string
	for _, j := range listed {
		listedIDs = append(listedIDs, j.Job.ID)
	}
	// legacy paused は wait を RENAME したものなので LIST ネイティブ順
	// (新しい順)。ascending は他の LIST bucket と同じく古い順に直す。
	assert.Equal(t, ids, listedIDs, "ListJobs(paused) must agree with Counts.Paused, oldest first")

	// Resume で吸い出したあとは v6 の意味論だけが残る。
	require.NoError(t, queue.Resume(ctx))

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "Resume must drain the legacy list")

	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, ids, waitMembers, "drained jobs must land in wait")

	counts, err = queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, counts.Paused, "nothing is held back once resumed")
	assert.EqualValues(t, 3, counts.Wait)

	listed, err = queue.ListJobs(ctx, mkq.JobBucketPaused, 0, -1, true)
	require.NoError(t, err)
	assert.Empty(t, listed, "a running queue holds nothing back, so paused is empty")
}

// TestQueue_Resume_DrainsLegacyPausedListInChunks covers the batching in
// pause-7.lua: it moves at most 7000 jobs per call so a long list cannot
// block Redis, and returns how many are left. Resume has to keep calling
// until that reaches zero, otherwise a queue that BullMQ 5 paused with a
// large backlog silently strands everything past the first chunk.
func TestQueue_Resume_DrainsLegacyPausedListInChunks(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// wait を非空にしておく。空だと Lua は RENAME の一発で済ませてしまい、
	// チャンク分割の経路を通らない。
	require.NoError(t, rdb.LPush(ctx, base+"wait", "sentinel").Err())

	// 7000 を 1 件超えさせる。ちょうど 2 ラウンド必要になる。
	const parked = 7001
	ids := make([]any, 0, parked)
	for i := range parked {
		ids = append(ids, fmt.Sprintf("legacy-%d", i))
	}
	require.NoError(t, rdb.RPush(ctx, base+"paused", ids...).Err())
	require.NoError(t, rdb.HSet(ctx, base+"meta", "paused", 1).Err())

	require.NoError(t, queue.Resume(ctx))

	pausedLen, err := rdb.LLen(ctx, base+"paused").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, pausedLen, "every chunk must be drained, not just the first 7000")

	waitLen, err := rdb.LLen(ctx, base+"wait").Result()
	require.NoError(t, err)
	assert.EqualValues(t, parked+1, waitLen, "all parked jobs plus the sentinel must be in wait")

	// wait の末尾が次に処理される側。paused リストの末尾 (= 最も古い
	// parked job) がそこへ来ていれば、2 ラウンドに割れても FIFO が保たれて
	// いる。逆順に積むと古いジョブが最後まで処理されない。
	next, err := rdb.LIndex(ctx, base+"wait", -1).Result()
	require.NoError(t, err)
	assert.Equal(t, "legacy-7000", next, "the oldest parked job must be the next one consumed")
}
