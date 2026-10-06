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
