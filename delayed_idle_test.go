package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/elythia-network/mkq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// **delayed job が 1 件でも積まれていると、アイドルのバックオフが効かない。**
//
// dispatchLoop は次の delayed job の時刻が分かると marker 待ちに入らず
// precise sleep する。その sleep は idlePollInterval を上限としているため、
// 1 時間後の job が 1 件あるだけで dispatcher は 100ms ごとに tryOnce を
// 撃ち続ける (worker 数に比例)。
//
// 実運用インスタンスで観測: deliver / maintenance に delayed が滞留した
// 状態で、marker 待ち由来の bzpopmin は毎秒 2.7 まで落ちているのに
// moveToActive の evalsha は毎秒 79 のままだった。
func TestIdleBackoff_NotDefeatedByDelayedJob(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	// 十分先の delayed job を 1 件だけ積む。テスト中に due にはならない。
	_, err := queue.Add(context.Background(), testPayload{Inbox: "far-future"},
		mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithConcurrency(8))
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Stop(ctx)
	}()

	// 起動直後は待ちが最短なので、伸びるまで待ってから測る。
	time.Sleep(4 * time.Second)
	before := redisCommandCount(t)
	time.Sleep(10 * time.Second)
	rate := (redisCommandCount(t) - before) / 10

	assert.Lessf(t, rate, int64(40),
		"delayed job があるとアイドル時のコマンド数が毎秒 %d。delayed 経路が待ちを伸ばしていない", rate)
	t.Logf("delayed job 1 件でアイドル時 毎秒 %d コマンド (worker 8)", rate)
}

// **遠い delayed job があっても、後から積んだ即時ジョブは待たされないこと。**
//
// delayed 経路で長く待つようにすると、marker で起きられなければ wait list に
// 積まれたジョブが次の delayed 時刻まで放置される。ここが崩れると、delayed が
// 1 件あるだけでキュー全体が止まる。
func TestDelayedJob_DoesNotDelayImmediatePickup(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	_, err := queue.Add(context.Background(), testPayload{Inbox: "far-future"},
		mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	got := make(chan time.Time, 1)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		if j.Data.Inbox == "wake" {
			select {
			case got <- time.Now():
			default:
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Stop(ctx)
	}()

	// 待ちが伸びきるのを待ってから積む。
	time.Sleep(8 * time.Second)

	enqueued := time.Now()
	_, err = queue.Add(context.Background(), testPayload{Inbox: "wake"})
	require.NoError(t, err)

	select {
	case at := <-got:
		elapsed := at.Sub(enqueued)
		assert.Lessf(t, elapsed, time.Second,
			"取得に %v かかった。delayed 待ち中に marker で起きていない", elapsed)
		t.Logf("delayed job があっても %v で取得", elapsed)
	case <-time.After(10 * time.Second):
		t.Fatal("delayed 待ちに入ったまま即時ジョブを取りに来ない")
	}
}

// **sub-second の delayed job は時刻どおりに動くこと。**
//
// marker 待ち (BZPOPMIN) は go-redis がタイムアウトを 1 秒へ切り上げるため、
// 50ms の retry backoff を marker 待ちに載せると 1 秒近く overshoot する。
// 短い delay は precise sleep のままでなければならない。
func TestDelayedJob_SubSecondPrecision(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	got := make(chan time.Time, 1)
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		select {
		case got <- time.Now():
		default:
		}
		return nil, nil
	})
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Stop(ctx)
	}()

	// 待ちが伸びきってから短い delayed job を積む。
	time.Sleep(8 * time.Second)

	const delay = 200 * time.Millisecond
	enqueued := time.Now()
	_, err = queue.Add(context.Background(), testPayload{Inbox: "soon"}, mkq.WithDelay(delay))
	require.NoError(t, err)

	select {
	case at := <-got:
		elapsed := at.Sub(enqueued)
		assert.Lessf(t, elapsed, 800*time.Millisecond,
			"delay %v の job が %v かかった。BZPOPMIN の 1 秒床で overshoot している", delay, elapsed)
		t.Logf("delay %v の job を %v で実行", delay, elapsed)
	case <-time.After(10 * time.Second):
		t.Fatal("sub-second delayed job が実行されない")
	}
}
