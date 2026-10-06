package mkq_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestWorker_Drain_LetsTheInFlightJobFinish is the whole point of
// Drain: the handler that is already running is not cancelled, so the
// job completes instead of being cut short and re-delivered later by
// stalled recovery.
func TestWorker_Drain_LetsTheInFlightJobFinish(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	entered := make(chan struct{})
	var sawCancel, finished atomic.Bool

	worker, err := mkq.Process(queue, func(hctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(entered)
		select {
		case <-time.After(400 * time.Millisecond):
			finished.Store(true)
		case <-hctx.Done():
			sawCancel.Store(true)
		}
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer drainCancel()
	require.NoError(t, worker.Drain(drainCtx))

	assert.False(t, sawCancel.Load(), "Drain must not cancel a running handler")
	assert.True(t, finished.Load(), "the handler must run to completion")

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	score, err := rdb.ZScore(ctx, base+"completed", job.ID).Result()
	require.NoError(t, err, "the drained job must be finalised as completed")
	assert.Greater(t, score, float64(0))
}

// Stop keeps its own contract: the handler is cancelled, not awaited on
// its own terms.
func TestWorker_Stop_StillCancelsTheInFlightJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	entered := make(chan struct{})
	var sawCancel atomic.Bool

	worker, err := mkq.Process(queue, func(hctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(entered)
		select {
		case <-time.After(10 * time.Second):
		case <-hctx.Done():
			sawCancel.Store(true)
		}
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	require.NoError(t, worker.Stop(stopCtx))
	assert.True(t, sawCancel.Load(), "Stop must cancel the running handler")
}

// Draining means taking no new work: a job queued while the worker is
// finishing its current one must be left for somebody else.
func TestWorker_Drain_TakesNoNewJobs(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Inbox: "first"})
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Int64

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		if handled.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// 1 つめを掴んだまま 2 つめを積む。dispatcher は 1 本きりで塞がって
	// いるので、Drain を呼ぶまでこれが拾われることはない。
	second, err := queue.Add(ctx, testPayload{Inbox: "second"})
	require.NoError(t, err)

	drainErr := make(chan error, 1)
	go func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer drainCancel()
		drainErr <- worker.Drain(drainCtx)
	}()

	// Drain が実際に走り出す猶予。ここが短すぎると「Drain 前に 1 つめが
	// 終わってしまい 2 つめを拾う」方向に倒れ、テストは失敗側に出る。
	time.Sleep(150 * time.Millisecond)
	close(release)

	select {
	case err := <-drainErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Drain never returned")
	}

	assert.EqualValues(t, 1, handled.Load(), "the second job must not be picked up")

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waiting, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	assert.Contains(t, waiting, second.ID, "the second job must be left in wait")

	active, err := rdb.LRange(ctx, base+"active", 0, -1).Result()
	require.NoError(t, err)
	assert.Empty(t, active, "nothing may be left stranded in active")
}

// A handler that outlives the grace period is cancelled exactly as Stop
// would: Drain stops being polite once its deadline passes.
func TestWorker_Drain_DeadlineFallsBackToCancelling(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	entered := make(chan struct{})
	cancelled := make(chan struct{})

	worker, err := mkq.Process(queue, func(hctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(entered)
		<-hctx.Done()
		close(cancelled)
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer drainCancel()
	err = worker.Drain(drainCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was never cancelled after the drain deadline")
	}
}

// Drain and Stop share one shutdown latch, so mixing them — as a
// shutdown helper invoked from more than one place would — is safe.
func TestWorker_Drain_ComposesWithStop(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, worker.Drain(ctx))
	require.NoError(t, worker.Drain(ctx))
	require.NoError(t, worker.Stop(ctx))
}

// freezableProxy sits between the client and Redis and can be told to
// stop forwarding, which is how a test gets a connection that is open
// but unresponsive — the state Redis is in when someone reaches for
// Stop with a short budget.
type freezableProxy struct {
	ln     net.Listener
	frozen atomic.Bool
	// blocked fires the first time a client->Redis write is held back
	// by the freeze, i.e. "the request is on the wire and the reply is
	// not coming". It is how a test pins the moment *between* building
	// a command and getting its answer.
	blocked   chan struct{}
	blockOnce sync.Once
}

func newFreezableProxy(t *testing.T, upstream string) *freezableProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &freezableProxy{ln: ln, blocked: make(chan struct{})}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(down, upstream)
		}
	}()
	return p
}

func (p *freezableProxy) serve(down net.Conn, upstream string) {
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		_ = down.Close()
		return
	}
	defer func() { _ = up.Close(); _ = down.Close() }()

	pipe := func(dst, src net.Conn, toUpstream bool) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				// 凍結中は読み取った分を止める。接続は開いたままなので、
				// クライアントは応答待ちでブロックする。
				for p.frozen.Load() {
					if toUpstream {
						p.blockOnce.Do(func() { close(p.blocked) })
					}
					time.Sleep(5 * time.Millisecond)
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	go pipe(up, down, true)
	pipe(down, up, false)
}

func (p *freezableProxy) addr() string { return p.ln.Addr().String() }
func (p *freezableProxy) freeze()      { p.frozen.Store(true) }
func (p *freezableProxy) thaw()        { p.frozen.Store(false) }

// blockedOnRequest returns a channel closed once a client request has
// been held by the freeze.
func (p *freezableProxy) blockedOnRequest() <-chan struct{} { return p.blocked }

// TestWorker_Stop_CancelsBeforeTalkingToRedis pins the ordering inside
// Stop. beginShutdown's wakeDispatchers issues a synchronous write
// bounded by a second, and "Redis is wedged" is exactly when someone
// reaches for Stop — the handlers must not sit uncancelled behind that
// round-trip.
func TestWorker_Stop_CancelsBeforeTalkingToRedis(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	proxy := newFreezableProxy(t, testRedisAddr())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{proxy.addr()}},
		KeyPrefix: prefix,
	})
	require.NoError(t, err)
	t.Cleanup(func() { flushPrefix(t, prefix); _ = c.Close() })

	queue := mkq.Define[testPayload](c, "deliver")
	_, err = queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	entered := make(chan struct{})
	cancelledAt := make(chan time.Time, 1)

	worker, err := mkq.Process(queue, func(hctx context.Context, _ *mkq.Job[testPayload]) (any, error) {
		close(entered)
		<-hctx.Done()
		cancelledAt <- time.Now()
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	// ここから Redis は「開いているが応答しない」。wakeDispatchers は
	// 自前の 1 秒タイムアウトまで戻らない。
	proxy.freeze()

	start := time.Now()
	go func() { _ = worker.Stop(context.Background()) }()

	select {
	case at := <-cancelledAt:
		assert.Less(t, at.Sub(start), 500*time.Millisecond,
			"the handler must be cancelled without waiting on the Redis round-trip (took %v)", at.Sub(start))
	case <-time.After(15 * time.Second):
		t.Fatal("the handler was never cancelled")
	}
}

// TestWorker_Drain_RunsAJobItAlreadyPrefetched covers the narrow window
// where a moveToFinished left with fetchNext=1 and the drain landed
// before the dispatcher got back to the top of its loop. The dispatcher
// is then holding a job that has already been moved wait->active and
// locked; dropping it would strand it in active until some other
// worker's stalled scan — and beginShutdown has just stopped this
// worker's own.
//
// **窓は普通なら EVAL 1 往復ぶんしかない。** プロキシを凍らせて
// moveToFinished を止めている間に Drain を入れることで、その一瞬を
// 手で開いている。
func TestWorker_Drain_RunsAJobItAlreadyPrefetched(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	proxy := newFreezableProxy(t, testRedisAddr())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{proxy.addr()}},
		KeyPrefix: prefix,
	})
	require.NoError(t, err)
	t.Cleanup(func() { flushPrefix(t, prefix); _ = c.Close() })

	queue := mkq.Define[testPayload](c, "deliver")
	first, err := queue.Add(ctx, testPayload{Inbox: "first"})
	require.NoError(t, err)
	second, err := queue.Add(ctx, testPayload{Inbox: "second"})
	require.NoError(t, err)

	firstDone := make(chan struct{})
	var handled atomic.Int64

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		if handled.Add(1) == 1 {
			// 次に走る Redis 往復が moveToFinished。そこで止める。
			proxy.freeze()
			close(firstDone)
		}
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)

	select {
	case <-firstDone:
	case <-time.After(15 * time.Second):
		t.Fatal("the first handler never ran")
	}

	// **ここが肝。** moveToFinished が実際にワイヤに出て、返事が来ない状態に
	// なるまで待つ。これで fetchNextFlag は cancel 前に評価済みだと確定する
	// ので、prefetch を掴んだまま Drain を受ける状況を手で作れる。
	select {
	case <-proxy.blockedOnRequest():
	case <-time.After(15 * time.Second):
		t.Fatal("the moveToFinished request never reached the frozen proxy")
	}

	drainErr := make(chan error, 1)
	go func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer drainCancel()
		drainErr <- worker.Drain(drainCtx)
	}()

	// beginShutdown は runCancel を先に撃つので、凍結中でも runCtx は
	// ここで cancel されている。その状態で EVAL を返させる。
	time.Sleep(200 * time.Millisecond)
	proxy.thaw()

	select {
	case err := <-drainErr:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Drain never returned")
	}

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	for _, id := range []string{first.ID, second.ID} {
		score, err := rdb.ZScore(ctx, base+"completed", id).Result()
		require.NoError(t, err, "job %s should have completed, not been stranded in active", id)
		assert.Greater(t, score, float64(0))
	}
	assert.EqualValues(t, 2, handled.Load())

	active, err := rdb.LRange(ctx, base+"active", 0, -1).Result()
	require.NoError(t, err)
	assert.Empty(t, active)
}
