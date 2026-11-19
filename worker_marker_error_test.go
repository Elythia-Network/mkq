package mkq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markerFailingHook fails every BZPOPMIN, like a proxy or ACL that rejects
// blocking commands, and counts the other commands the client sends.
type markerFailingHook struct {
	others atomic.Int64
	// allow lets that many BZPOPMIN through. passed counts the ones that
	// went through and returned.
	allow  atomic.Int64
	passed atomic.Int64

	mu       sync.Mutex
	failedAt []time.Time
}

func (h *markerFailingHook) failures() []time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Time(nil), h.failedAt...)
}

func (h *markerFailingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *markerFailingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "bzpopmin" {
			h.others.Add(1)
			return next(ctx, cmd)
		}
		if h.allow.Load() > 0 && h.allow.Add(-1) >= 0 {
			err := next(ctx, cmd)
			h.passed.Add(1)
			return err
		}
		h.mu.Lock()
		h.failedAt = append(h.failedAt, time.Now())
		h.mu.Unlock()
		err := errors.New("ERR blocking commands are not allowed")
		cmd.SetErr(err)
		return err
	}
}

func (h *markerFailingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.others.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// TestWorker_MarkerWaitFailureBacksOff pins #159: when only BZPOPMIN fails,
// an idle dispatcher sleeps between attempts instead of retrying the
// dequeue at once.
func TestWorker_MarkerWaitFailureBacksOff(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &markerFailingHook{}
	c.rdb.AddHook(hook)
	adder := internalTestClient(t, prefix)

	type payload struct {
		N int `json:"n"`
	}
	got := make(chan struct{}, 1)
	worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
		select {
		case got <- struct{}{}:
		default:
		}
		return nil, nil
	}, WithConcurrency(1), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	time.Sleep(200 * time.Millisecond)
	before := hook.others.Load()
	time.Sleep(3 * time.Second)
	// 既定の間隔 (100ms) から倍にして 5 秒で頭打ち: 3 秒で眠りは 100, 200, 400,
	// 800, 1600ms の 5 回ほど。眠らなければ、3 秒で数千回取り出しを撃つ。
	sent := hook.others.Load() - before
	assert.Less(t, sent, int64(60), "commands sent while idle with a failing BZPOPMIN")

	// 眠っている間に積んだジョブも、眠りが明ければ取る (取り出しは動く)。
	_, err = Define[payload](adder, "deliver").Add(context.Background(), payload{N: 1})
	require.NoError(t, err)
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was not picked up")
	}
}

// logRecorder records the messages logged at Warn and Info.
type logRecorder struct {
	mu    sync.Mutex
	warns []string
	infos []string
}

func (r *logRecorder) Debug(string, ...slog.Attr) {}
func (r *logRecorder) Error(string, ...slog.Attr) {}
func (r *logRecorder) Info(msg string, _ ...slog.Attr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.infos = append(r.infos, msg)
}
func (r *logRecorder) Warn(msg string, _ ...slog.Attr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warns = append(r.warns, msg)
}

func (r *logRecorder) count(infos bool, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	list := r.warns
	if infos {
		list = r.infos
	}
	n := 0
	for _, m := range list {
		if m == msg {
			n++
		}
	}
	return n
}

// TestWorker_MarkerWaitFailureSleepDoubles pins that the sleeps after a
// failing BZPOPMIN grow, so a persistent failure settles at errorWaitMax,
// and that only the first failure of the run is logged as a warning.
func TestWorker_MarkerWaitFailureSleepDoubles(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &markerFailingHook{}
	c.rdb.AddHook(hook)
	rec := &logRecorder{}
	c.logger = rec

	type payload struct {
		N int `json:"n"`
	}
	worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
		return nil, nil
	}, WithConcurrency(1), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	// 100, 200, 400, 800ms と眠るので、4 秒で 5 回目まで届く。
	require.Eventually(t, func() bool { return len(hook.failures()) >= 5 }, 4*time.Second, 20*time.Millisecond)
	at := hook.failures()
	first, fourth := at[1].Sub(at[0]), at[4].Sub(at[3])
	assert.Less(t, first, 400*time.Millisecond, "the first sleep is short")
	assert.GreaterOrEqual(t, fourth, 700*time.Millisecond, "the sleep doubles (fourth gap)")
	assert.Equal(t, 1, rec.count(false, "mkq: marker wait failed"), "only the first failure of the run warns")
}

// TestWorker_MarkerWaitTimeoutIsNotAFailure pins that an idle BZPOPMIN that
// times out (redis.Nil) is not treated as a failure: no warning, no back-off.
func TestWorker_MarkerWaitTimeoutIsNotAFailure(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	rec := &logRecorder{}
	c.logger = rec

	type payload struct {
		N int `json:"n"`
	}
	worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
		return nil, nil
	}, WithConcurrency(1), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	// 待ちは 1 秒から倍になるので、3.5 秒で 2 回はタイムアウトする。
	time.Sleep(3500 * time.Millisecond)
	assert.Zero(t, rec.count(false, "mkq: marker wait failed"))
}

// TestWorker_MarkerWaitFailureSleepResets pins that a BZPOPMIN that works
// again resets the sleep (the next failure starts from the short sleep) and
// logs the recovery.
func TestWorker_MarkerWaitFailureSleepResets(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &markerFailingHook{}
	c.rdb.AddHook(hook)
	rec := &logRecorder{}
	c.logger = rec

	type payload struct {
		N int `json:"n"`
	}
	worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
		return nil, nil
	}, WithConcurrency(1), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	// 失敗を 5 回続けて、眠りを 1.6 秒まで伸ばす。
	require.Eventually(t, func() bool { return len(hook.failures()) >= 5 }, 4*time.Second, 20*time.Millisecond)
	// BZPOPMIN を 1 回だけ通し、返ってくる (タイムアウト = 成功) まで待つ。
	hook.allow.Store(1)
	require.Eventually(t, func() bool { return hook.passed.Load() >= 1 }, 15*time.Second, 20*time.Millisecond)
	// 次からはまた失敗する。ここから先の失敗は、どれも戻った後のもの。
	// 眠りが戻っていれば 100ms、200ms と眠るので、2 回目までの間隔は短い。
	n := len(hook.failures())
	require.Eventually(t, func() bool { return len(hook.failures()) >= n+2 }, 6*time.Second, 20*time.Millisecond)
	at := hook.failures()
	assert.Less(t, at[n+1].Sub(at[n]), time.Second, "the sleep restarts from the short one")
	assert.Equal(t, 1, rec.count(true, "mkq: marker wait recovered"))
	assert.Equal(t, 2, rec.count(false, "mkq: marker wait failed"), "each run of failures warns once")
}

// TestWorker_MarkerWaitFailureDoesNotSleepPastDelayedJob pins that the sleep
// after a failing BZPOPMIN is cut at the due time of the next delayed job.
func TestWorker_MarkerWaitFailureDoesNotSleepPastDelayedJob(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &markerFailingHook{}
	c.rdb.AddHook(hook)
	adder := internalTestClient(t, prefix)

	type payload struct {
		N int `json:"n"`
	}
	got := make(chan time.Time, 1)
	worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
		select {
		case got <- time.Now():
		default:
		}
		return nil, nil
	}, WithConcurrency(1), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	// 眠りを上限 (5 秒) まで伸ばす: 100+200+400+800+1600+3200ms の後の 7 回目。
	require.Eventually(t, func() bool { return len(hook.failures()) >= 7 }, 12*time.Second, 5*time.Millisecond)
	// worker は今 5 秒眠っている。7 秒後に期限の来るジョブを積むと、起きた時点で
	// 期限まで約 2 秒あり、その marker 待ちがまた失敗する。眠りを期限で切れば
	// 期限どおりに取り、切らなければ 5 秒眠って約 3 秒遅れる。
	const delay = 7 * time.Second
	due := time.Now().Add(delay)
	_, err = Define[payload](adder, "deliver").Add(context.Background(), payload{N: 1}, WithDelay(delay))
	require.NoError(t, err)
	select {
	case at := <-got:
		assert.Less(t, at.Sub(due), 1500*time.Millisecond, "picked up %v after the due time", at.Sub(due))
	case <-time.After(20 * time.Second):
		t.Fatal("the delayed job was not picked up")
	}
}
