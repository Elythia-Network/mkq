package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/elythia-network/mkq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// **同じキューで別の worker が動き続けていても Stop が返ること。**
//
// Stop は待機中の dispatcher を Redis 経由で起こす。起こす先が共有の marker
// key だと、生き残る worker の dispatcher が起き上がって push を横取りし、
// 止めたい worker の dispatcher が取り残される。
//
// 実際にこれで本番が起動しなくなった。オートスケーラが inbox を 16 → 4 に
// 縮める過程で Worker.Stop が返らず、Server.Start が完了せず HTTP の listen
// まで到達しなかった (mk-go #2602)。
//
// delayed job を 1 件積んでおくのは、dispatcher を最初から長い待ちに入らせる
// ため。アイドルのバックオフが伸びるのを待たずに再現できる。
func TestStop_WakesOnlyOwnDispatchers(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	_, err := queue.Add(context.Background(), testPayload{Inbox: "far-future"},
		mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	noop := func(context.Context, *mkq.Job[testPayload]) (any, error) { return nil, nil }

	// 生き残る側。止める側と同じキューを見ている。
	survivor, err := mkq.Process(queue, noop, mkq.WithConcurrency(4))
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = survivor.Stop(ctx)
	}()

	victim, err := mkq.Process(queue, noop, mkq.WithConcurrency(4))
	require.NoError(t, err)

	// 全 dispatcher が BZPopMin に入るのを待つ。
	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	err = victim.Stop(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Lessf(t, elapsed, 3*time.Second,
		"Stop に %v かかった。生き残る worker に起こしを横取りされている", elapsed)
	t.Logf("同じキューに別 worker がいても Stop %v", elapsed.Round(time.Millisecond))
}

// **止めた worker の wake key を残さないこと。**
//
// 起こしは dispatcher 数だけ push するが、ジョブ実行中などで BZPopMin に
// いない分は消費されずに余る。Worker ごとに key を分けた以上、消さないと
// Redis に孤児の key が積み上がる。
func TestStop_RemovesWakeKey(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithConcurrency(4))
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(ctx))

	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer rdb.Close()
	keys, err := rdb.Keys(context.Background(), prefix+":deliver:mkq:wake:*").Result()
	require.NoError(t, err)
	assert.Emptyf(t, keys, "wake key が残っている: %v", keys)
}

// **期限を待っていた worker を止めても、遅延ジョブが期限どおりに処理されること (#150)。**
//
// delayed job を積んだときの marker は 1 つの dispatcher だけが取り、期限まで
// 待つのはその dispatcher だけになる。それが止まると、残りの worker は待機の
// 間隔 (ここでは 20 秒) を待ち終えるまでジョブを promote しなかった。
//
// Redis は同じ key で待つクライアントを、待ち始めた順に起こす。先に起動した
// first が marker を取って期限を待ち、second は待機の間隔で眠ったままになる。
func TestStop_HandsOffDelayedWait(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	done := make(chan string, 1)
	handler := func(name string) func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return func(context.Context, *mkq.Job[testPayload]) (any, error) {
			select {
			case done <- name:
			default:
			}
			return nil, nil
		}
	}
	opts := []mkq.WorkerOption{mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20 * time.Second)}

	first, err := mkq.Process(queue, handler("first"), opts...)
	require.NoError(t, err)
	time.Sleep(time.Second)
	second, err := mkq.Process(queue, handler("second"), opts...)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = second.Stop(ctx)
	}()
	time.Sleep(time.Second)

	const delay = 3 * time.Second
	added := time.Now()
	_, err = queue.Add(context.Background(), testPayload{Inbox: "soon"}, mkq.WithDelay(delay))
	require.NoError(t, err)

	// first が marker を取って期限の待ちに入るのを待ってから止める。
	time.Sleep(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, first.Stop(ctx))

	select {
	case name := <-done:
		elapsed := time.Since(added)
		assert.Equal(t, "second", name)
		assert.Lessf(t, elapsed, delay+2*time.Second,
			"期限の %v に対して %v 後に処理された。期限の待ちが引き継がれていない", delay, elapsed)
		t.Logf("期限 %v の遅延ジョブを %v 後に処理", delay, elapsed.Round(time.Millisecond))
	case <-time.After(delay + 5*time.Second):
		t.Fatalf("期限の %v を 5 秒過ぎても処理されない。期限の待ちが引き継がれていない", delay)
	}
}

// **止まる worker の handler が長引いても、期限の待ちの引き継ぎは遅れないこと (#150)。**
//
// Drain は実行中の handler が終わるまで返らない。引き継ぎを dispatcher が
// すべて戻った後に置くと、その間に期限が来ても誰も promote しない。
func TestDrain_HandsOffDelayedWaitBeforeHandlersFinish(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan string, 1)
	handler := func(name string) func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return func(_ context.Context, job *mkq.Job[testPayload]) (any, error) {
			if job.Data.Inbox == "block" {
				close(started)
				<-release
				return nil, nil
			}
			select {
			case done <- name:
			default:
			}
			return nil, nil
		}
	}
	opts := []mkq.WorkerOption{mkq.WithIdlePollInterval(20 * time.Second)}

	// first の 1 つの dispatcher が block を処理し続け、もう 1 つが期限を待つ。
	// moveToActive はジョブを取ると marker を置き直して次の dispatcher を起こす
	// ので、block を積むと first のもう 1 つも起きて待ち直す。second はその後に
	// 起動し、first の残りより後に待ち始めさせる。
	first, err := mkq.Process(queue, handler("first"), append(opts, mkq.WithConcurrency(2))...)
	require.NoError(t, err)
	time.Sleep(time.Second)
	_, err = queue.Add(context.Background(), testPayload{Inbox: "block"})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking handler never started")
	}
	time.Sleep(time.Second)

	second, err := mkq.Process(queue, handler("second"), append(opts, mkq.WithConcurrency(1))...)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = second.Stop(ctx)
	}()
	time.Sleep(time.Second)

	const delay = 3 * time.Second
	added := time.Now()
	_, err = queue.Add(context.Background(), testPayload{Inbox: "soon"}, mkq.WithDelay(delay))
	require.NoError(t, err)
	time.Sleep(time.Second)

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		drained <- first.Drain(ctx)
	}()
	defer func() {
		close(release)
		require.NoError(t, <-drained)
	}()

	select {
	case name := <-done:
		elapsed := time.Since(added)
		assert.Equal(t, "second", name)
		assert.Lessf(t, elapsed, delay+2*time.Second,
			"期限の %v に対して %v 後に処理された。期限の待ちの引き継ぎが handler の完了を待っている", delay, elapsed)
		t.Logf("期限 %v の遅延ジョブを %v 後に処理", delay, elapsed.Round(time.Millisecond))
	case <-time.After(delay + 5*time.Second):
		t.Fatalf("期限の %v を 5 秒過ぎても処理されない。期限の待ちの引き継ぎが handler の完了を待っている", delay)
	}
}

// **止まる途中の dispatcher が取った wait の job の marker を、別の worker へ
// 引き継ぐこと (#150)。**
//
// 止まり始めてから wake key への push が届くまでの間に job が積まれると、止まる
// 途中の dispatcher がその marker を取って捨てる。job は wait に残り、他の worker
// は待機の間隔 (ここでは 20 秒) まで気付かない。窓は 1 往復ほどで狙って開けられ
// ないので、pause の間に積んで marker の無い job を作り、同じ状態を再現する。
func TestStop_HandsOffWaitingJob(t *testing.T) {
	cases := []struct {
		name string
		opts []mkq.AddOption
	}{
		{"wait", nil},
		{"prioritized", []mkq.AddOption{mkq.WithPriority(1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefix := uniquePrefix(t)
			c := newClient(t, prefix)
			queue := mkq.Define[testPayload](c, "deliver")

			done := make(chan string, 1)
			handler := func(name string) func(context.Context, *mkq.Job[testPayload]) (any, error) {
				return func(context.Context, *mkq.Job[testPayload]) (any, error) {
					select {
					case done <- name:
					default:
					}
					return nil, nil
				}
			}
			opts := []mkq.WorkerOption{mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20 * time.Second)}

			victim, err := mkq.Process(queue, handler("victim"), opts...)
			require.NoError(t, err)
			survivor, err := mkq.Process(queue, handler("survivor"), opts...)
			require.NoError(t, err)
			defer stopWorker(t, survivor)
			time.Sleep(time.Second)

			ctx := context.Background()
			require.NoError(t, queue.Pause(ctx))
			_, err = queue.Add(ctx, testPayload{Inbox: "waiting"}, tc.opts...)
			require.NoError(t, err)
			// resume は marker を置くので使わず、paused の印だけを消す。
			rdb := rawClient(t)
			require.NoError(t, rdb.HDel(ctx, prefix+":deliver:meta", "paused").Err())
			n, err := rdb.ZCard(ctx, prefix+":deliver:marker").Result()
			require.NoError(t, err)
			require.Zero(t, n, "the job must have no marker before the stop")

			start := time.Now()
			stopWorker(t, victim)

			select {
			case name := <-done:
				assert.Equal(t, "survivor", name)
				assert.Less(t, time.Since(start), 3*time.Second)
			case <-time.After(8 * time.Second):
				t.Fatal("the waiting job was not handed off to the surviving worker")
			}
		})
	}
}

// **paused のキューでは、止めても wait の job の marker を置かないこと。**
//
// BullMQ の addBaseMarkerIfNeeded と同じく、paused か concurrency の上限に
// 達しているときは置かない。置くと、他の worker が起きて空振りする。
func TestStop_DoesNotMarkWaitingJobOfPausedQueue(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Second))
	require.NoError(t, err)
	time.Sleep(time.Second)

	ctx := context.Background()
	require.NoError(t, queue.Pause(ctx))
	_, err = queue.Add(ctx, testPayload{Inbox: "waiting"})
	require.NoError(t, err)
	stopWorker(t, worker)

	rdb := rawClient(t)
	_, err = rdb.ZScore(ctx, prefix+":deliver:marker", "0").Result()
	assert.ErrorIs(t, err, redis.Nil, "a paused queue must not get a marker for its waiting job")
}
