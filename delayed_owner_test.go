package mkq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markerWaitHook records when each BZPOPMIN is sent.
type markerWaitHook struct {
	mu sync.Mutex
	at []time.Time
}

func (h *markerWaitHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *markerWaitHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "bzpopmin") {
			h.mu.Lock()
			h.at = append(h.at, time.Now())
			h.mu.Unlock()
		}
		return next(ctx, cmd)
	}
}

func (h *markerWaitHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *markerWaitHook) since(t time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, at := range h.at {
		if !at.Before(t) {
			n++
		}
	}
	return n
}

type ownerPayload struct {
	N int `json:"n"`
}

// startOwnerWorker starts a Worker that sends the time each job runs to got.
func startOwnerWorker(t *testing.T, c *Client, concurrency int, got chan<- time.Time) *Worker {
	t.Helper()
	worker, err := Process(Define[ownerPayload](c, "deliver"), func(context.Context, *Job[ownerPayload]) (any, error) {
		got <- time.Now()
		return nil, nil
	}, WithConcurrency(concurrency), WithStalledInterval(0), WithIdlePollInterval(time.Minute))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})
	return worker
}

// waitParked waits until all of w's dispatchers sit in the marker wait and
// the marker key is empty, so that the delay marker has been taken.
//
// 2 回続けて成り立つまで待つ。BZPOPMIN が marker を取ってから数を減らす
// までの間も、1 回だけなら成り立ってしまう。
func waitParked(t *testing.T, w *Worker, concurrency int) {
	t.Helper()
	settled := false
	require.Eventually(t, func() bool {
		w.markerMu.Lock()
		parked := w.markerWaiters == concurrency
		w.markerMu.Unlock()
		n, err := w.rdb.ZCard(context.Background(), w.keys.marker).Result()
		ok := parked && err == nil && n == 0
		done := settled && ok
		settled = ok
		return done
	}, 10*time.Second, 200*time.Millisecond)
}

// TestWorker_OnlyDelayMarkerTakerWaitsForDeadline pins #150: when a delayed
// job falls due, only the dispatcher that took the delay marker wakes for
// it, not every dispatcher that has seen the deadline.
func TestWorker_OnlyDelayMarkerTakerWaitsForDeadline(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &markerWaitHook{}
	c.rdb.AddHook(hook)

	// 先に積むので、どの dispatcher も最初の取り出しで期限を知る。以前は
	// 16 本すべてが期限まで待ち、期限に 16 本が marker 待ちを撃ち直していた。
	const delay = 8 * time.Second
	due := time.Now().Add(delay)
	_, err := Define[ownerPayload](c, "deliver").Add(context.Background(), ownerPayload{N: 1}, WithDelay(delay))
	require.NoError(t, err)

	got := make(chan time.Time, 1)
	const concurrency = 8
	for range 2 {
		startOwnerWorker(t, c, concurrency, got)
	}
	select {
	case at := <-got:
		assert.Less(t, at.Sub(due), time.Second, "picked up %v after the due time", at.Sub(due))
	case <-time.After(delay + 10*time.Second):
		t.Fatal("the delayed job was not picked up")
	}
	time.Sleep(2 * time.Second)
	// 担当は、積んだときと Worker の起動で置いた marker を取った 2 本まで。
	// 期限に起きた担当と、引き継ぎの marker と promote で置かれた "0" を
	// 取った dispatcher が marker 待ちに戻る。4 はちょうどその最悪の値で、
	// marker を置く経路が増えると超える。
	assert.LessOrEqual(t, hook.since(due.Add(-time.Second)), 4,
		"dispatchers that did not take the delay marker woke for the deadline")
}

// TestWorker_DelayOwnerHandsOffOnAcquire pins #150: a dispatcher waiting
// for a deadline puts the delay marker back when it takes another job.
func TestWorker_DelayOwnerHandsOffOnAcquire(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	queue := Define[ownerPayload](c, "deliver")

	started := make(chan struct{})
	release := make(chan struct{})
	worker, err := Process(queue, func(_ context.Context, job *Job[ownerPayload]) (any, error) {
		if job.Data.N == 2 {
			close(started)
			<-release
		}
		return nil, nil
	}, WithConcurrency(1), WithStalledInterval(0), WithIdlePollInterval(time.Minute))
	require.NoError(t, err)
	t.Cleanup(func() {
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})
	waitParked(t, worker, 1)

	_, err = queue.Add(context.Background(), ownerPayload{N: 1}, WithDelay(time.Hour))
	require.NoError(t, err)
	// dispatcher が積んだときの marker を取り、期限を待ち始めるまで待つ。
	waitParked(t, worker, 1)
	next, err := c.rdb.ZRangeWithScores(context.Background(), worker.keys.delayed, 0, 0).Result()
	require.NoError(t, err)
	require.Len(t, next, 1)

	_, err = queue.Add(context.Background(), ownerPayload{N: 2})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting job was not picked up")
	}
	score, err := c.rdb.ZScore(context.Background(), worker.keys.marker, "1").Result()
	require.NoError(t, err, "the delay marker was not put back")
	assert.Equal(t, int64(next[0].Score)/0x1000, int64(score))
}

// TestWorker_RestoresDelayMarkerOnStart pins #150: a Worker puts the delay
// marker back when it starts, in case the dispatcher that held it is gone.
func TestWorker_RestoresDelayMarkerOnStart(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)

	const delay = 4 * time.Second
	due := time.Now().Add(delay)
	queue := Define[ownerPayload](c, "deliver")
	_, err := queue.Add(context.Background(), ownerPayload{N: 1}, WithDelay(delay))
	require.NoError(t, err)
	// 担当が引き継がずに消えた状態にする。
	require.NoError(t, c.rdb.Del(context.Background(), queue.keys.Marker()).Err())

	got := make(chan time.Time, 1)
	startOwnerWorker(t, c, 1, got)
	select {
	case at := <-got:
		assert.Less(t, at.Sub(due), time.Second, "picked up %v after the due time", at.Sub(due))
	case <-time.After(delay + 10*time.Second):
		t.Fatal("the delayed job was not picked up before the idle wait ran out")
	}
}

// TestWorker_DelayOwnerKeepsWaitingWithoutHandOff pins #150: a dispatcher
// that reaches its deadline but finds the job gone keeps waiting for the
// next deadline, since nobody handed the wait to another dispatcher.
func TestWorker_DelayOwnerKeepsWaitingWithoutHandOff(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	queue := Define[ownerPayload](c, "deliver")

	const first, second = 3 * time.Second, 6 * time.Second
	start := time.Now()
	due := start.Add(second)
	removed, err := queue.Add(context.Background(), ownerPayload{N: 1}, WithDelay(first))
	require.NoError(t, err)
	_, err = queue.Add(context.Background(), ownerPayload{N: 2}, WithDelay(second))
	require.NoError(t, err)

	// 最初の期限の 0.9 秒前に起動する。marker を取って担当になった後、期限
	// までが markerWaitFloor 未満なので、marker 待ちに入らずに期限まで眠る。
	// その間に、担当でない誰かに取られた状態と同じく、marker を置かずに
	// 消す。眠る前に消すと、担当は期限の前の取り出しで次の期限を知って
	// しまい、期限を過ぎた後の判定を通らない。
	time.Sleep(time.Until(start.Add(first - 900*time.Millisecond)))
	got := make(chan time.Time, 1)
	worker := startOwnerWorker(t, c, 1, got)
	require.Eventually(t, func() bool {
		worker.markerMu.Lock()
		defer worker.markerMu.Unlock()
		return worker.delayOwners.Load() == 1 && worker.markerWaiters == 0
	}, 500*time.Millisecond, 5*time.Millisecond)
	// marker を取った直後 (取り出しの前) でも成り立つので、取り出しを待つ。
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, queue.RemoveJob(context.Background(), removed.ID))
	require.True(t, time.Now().Before(start.Add(first)), "removed after the first deadline")

	select {
	case at := <-got:
		assert.Less(t, at.Sub(due), time.Second, "picked up %v after the due time", at.Sub(due))
	case <-time.After(second + 10*time.Second):
		t.Fatal("the delayed job was not picked up before the idle wait ran out")
	}
}

// TestQueue_ResumeRestoresDelayMarker pins #150: Resume puts the delay
// marker back, which Pause removed, so a delayed job left after the
// waiting ones is still picked up when due.
func TestQueue_ResumeRestoresDelayMarker(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	queue := Define[ownerPayload](c, "deliver")

	const delay = 6 * time.Second
	due := time.Now().Add(delay)
	_, err := queue.Add(context.Background(), ownerPayload{N: 1}, WithDelay(delay))
	require.NoError(t, err)

	got := make(chan time.Time, 2)
	worker := startOwnerWorker(t, c, 1, got)
	waitParked(t, worker, 1)
	require.NoError(t, queue.Pause(context.Background()))
	// 一時停止の間に積んだジョブがあると、pause-7.lua の resume は "0" だけを
	// 置く。
	_, err = queue.Add(context.Background(), ownerPayload{N: 2})
	require.NoError(t, err)
	// 担当が一時停止のキューで取り出し、担当を手放すまで待つ。実際には、
	// 一時停止の間に担当が待ちの上限か期限で起きたときにこうなる。この手順が
	// 無いと、担当は Resume の "0" で起きて引き継ぐので、直していなくても
	// 通ってしまう。
	require.NoError(t, worker.rdb.ZAdd(context.Background(), queue.keys.Marker(), redis.Z{Score: 0, Member: "0"}).Err())
	require.Eventually(t, func() bool { return worker.delayOwners.Load() == 0 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, queue.Resume(context.Background()))

	for range 2 {
		select {
		case at := <-got:
			if at.After(due) {
				assert.Less(t, at.Sub(due), time.Second, "picked up %v after the due time", at.Sub(due))
			}
		case <-time.After(delay + 10*time.Second):
			t.Fatal("the jobs were not picked up before the idle wait ran out")
		}
	}
}

// TestWorker_NonOwnerWaitsFullCapWithDelayedJob pins #150: a dispatcher
// that knows of a pending delayed job but does not hold the delay marker
// waits the full cap at once, rather than growing its idle wait from the
// short IdlePollInterval.
func TestWorker_NonOwnerWaitsFullCapWithDelayedJob(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	queue := Define[ownerPayload](c, "deliver")
	_, err := queue.Add(context.Background(), ownerPayload{N: 1}, WithDelay(time.Hour))
	require.NoError(t, err)

	hook := &countingHook{}
	c.rdb.AddHook(hook)
	const concurrency = 2
	worker, err := Process(queue, func(context.Context, *Job[ownerPayload]) (any, error) {
		return nil, nil
	}, WithConcurrency(concurrency), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	// 2 本とも marker 待ちの中にいて、前に見たときから 1 件も撃っていなければ、
	// 最初の取り出しの後の待ちに入ったとみなす。
	last := int64(-1)
	require.Eventually(t, func() bool {
		worker.markerMu.Lock()
		parked := worker.markerWaiters == concurrency
		worker.markerMu.Unlock()
		n := hook.n.Load()
		settled := parked && n == last
		last = n
		return settled
	}, 10*time.Second, 300*time.Millisecond)
	require.Equal(t, int64(1), worker.delayOwners.Load())
	before := hook.n.Load()
	// 既定の間隔 (100ms) から伸ばし直すと、BZPOPMIN の 1 秒の下限で 1 秒
	// ごとに起きる。
	time.Sleep(4 * time.Second)
	assert.Zero(t, hook.n.Load()-before, "the dispatcher without the delay marker woke before the cap")
}
