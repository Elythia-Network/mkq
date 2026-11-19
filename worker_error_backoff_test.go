package mkq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorWait_StartsShortAndCaps(t *testing.T) {
	assert.Equal(t, 100*time.Millisecond, firstErrorWait(100*time.Millisecond), "a short idle interval is kept")
	assert.Equal(t, errorWaitStart, firstErrorWait(2*time.Minute), "a long idle interval does not stretch the error sleep")
	assert.Equal(t, defaultIdlePollInterval, firstErrorWait(0), "a zero interval does not hot-loop")

	wait := firstErrorWait(2 * time.Minute)
	var seen []time.Duration
	for range 6 {
		seen = append(seen, wait)
		wait = nextErrorWait(wait)
	}
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, errorWaitMax, errorWaitMax, errorWaitMax}, seen)
}

// failingHook fails every command while on is set, like a Redis outage.
// failures counts the failed commands other than BZPOPMIN, so a test that
// waits for it knows a dequeue attempt (not just the marker wait) failed.
type failingHook struct {
	on       atomic.Bool
	failures atomic.Int64
}

var errInjected = errors.New("injected redis failure")

func (h *failingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on.Load() {
			if cmd.Name() != "bzpopmin" {
				h.failures.Add(1)
			}
			cmd.SetErr(errInjected)
			return errInjected
		}
		return next(ctx, cmd)
	}
}

func (h *failingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.on.Load() {
			h.failures.Add(1)
			for _, cmd := range cmds {
				cmd.SetErr(errInjected)
			}
			return errInjected
		}
		return next(ctx, cmds)
	}
}

func internalTestClient(t *testing.T, prefix string) *Client {
	t.Helper()
	addr := os.Getenv("MKQ_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClient(ctx, Config{Redis: redis.UniversalOptions{Addrs: []string{addr}}, KeyPrefix: prefix})
	require.NoError(t, err)
	t.Cleanup(func() {
		raw := redis.NewClient(&redis.Options{Addr: addr})
		defer raw.Close()
		bg, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		iter := raw.Scan(bg, 0, prefix+":*", 100).Iterator()
		for iter.Next(bg) {
			if err := raw.Del(bg, iter.Val()).Err(); err != nil {
				t.Errorf("cleanup del %s: %v", iter.Val(), err)
			}
		}
		if err := iter.Err(); err != nil {
			t.Errorf("cleanup scan %s: %v", prefix, err)
		}
		_ = c.Close()
	})
	return c
}

// TestWorker_ErrorSleepDoesNotFollowIdleInterval pins #154: with a long
// IdlePollInterval, a dispatcher whose dequeue failed resumes within
// seconds once Redis is back, instead of sleeping the idle interval.
func TestWorker_ErrorSleepDoesNotFollowIdleInterval(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &failingHook{}
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
	}, WithConcurrency(1), WithIdlePollInterval(2*time.Minute), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		hook.on.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	// dispatcher が marker 待ちに入ってから Redis を落とす。積んだジョブの
	// marker で起き、取り出しが失敗する。
	time.Sleep(500 * time.Millisecond)
	hook.on.Store(true)
	_, err = Define[payload](adder, "deliver").Add(context.Background(), payload{N: 1})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return hook.failures.Load() > 0 }, 5*time.Second, 10*time.Millisecond,
		"the dispatcher must hit the outage")

	recovered := time.Now()
	hook.on.Store(false)
	select {
	case at := <-got:
		// 最初の眠りは 1 秒、続けて失敗していれば 2 秒。idlePollInterval の 2 分ではない。
		assert.Less(t, at.Sub(recovered), 4*time.Second)
	case <-time.After(15 * time.Second):
		t.Fatal("the job was not picked up after Redis came back")
	}
}

// TestWorker_ErrorSleepResetsAfterSuccess pins that a successful dequeue
// resets the error sleep: a second outage after a long one starts again from
// the short sleep, not from errorWaitMax.
func TestWorker_ErrorSleepResetsAfterSuccess(t *testing.T) {
	t.Parallel()
	prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
	c := internalTestClient(t, prefix)
	hook := &failingHook{}
	c.rdb.AddHook(hook)
	adder := internalTestClient(t, prefix)

	type payload struct {
		N int `json:"n"`
	}
	got := make(chan time.Time, 4)
	worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
		got <- time.Now()
		return nil, nil
	}, WithConcurrency(1), WithIdlePollInterval(2*time.Minute), WithStalledInterval(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		hook.on.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})
	queue := Define[payload](adder, "deliver")
	outage := func(length time.Duration) time.Duration {
		t.Helper()
		before := hook.failures.Load()
		hook.on.Store(true)
		_, err := queue.Add(context.Background(), payload{N: 1})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return hook.failures.Load() > before }, 5*time.Second, 10*time.Millisecond)
		time.Sleep(length)
		recovered := time.Now()
		hook.on.Store(false)
		select {
		case at := <-got:
			return at.Sub(recovered)
		case <-time.After(15 * time.Second):
			t.Fatal("the job was not picked up after Redis came back")
			return 0
		}
	}

	time.Sleep(500 * time.Millisecond)
	// 1 回目は長く落とし、眠りを上限まで伸ばす (1+2+4 秒)。
	outage(7500 * time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	// 2 回目はすぐ戻す。最初の眠り (1 秒) で戻るはずで、上限の 5 秒ではない。
	assert.Less(t, outage(0), 3*time.Second)
}
