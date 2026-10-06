package mkq_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/elythia-network/mkq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisCommandCount reads total_commands_processed from INFO stats.
func redisCommandCount(t *testing.T) int64 {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer rdb.Close()
	info, err := rdb.Info(context.Background(), "stats").Result()
	require.NoError(t, err)
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "total_commands_processed:"); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			require.NoError(t, err)
			return n
		}
	}
	t.Fatal("total_commands_processed not found in INFO stats")
	return 0
}

// アイドルが続いても marker 待ちが伸びるので、空振りの tryOnce が減ること。
//
// **これが無いと worker 数に比例して Redis を叩き続ける。** 実運用の 44 worker
// 構成でアイドル時に毎秒 774 コマンド撃っていた (BullMQ は毎秒 21.5)。
func TestIdleBackoff_ReducesPolling(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

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

	// バックオフ無しなら worker 8 個で毎秒 60 前後。伸びていれば大きく下回る。
	assert.Lessf(t, rate, int64(40),
		"アイドル時のコマンド数が毎秒 %d。marker 待ちが伸びていない", rate)
	t.Logf("アイドル時 毎秒 %d コマンド (worker 8)", rate)
}

// **バックオフが伸びきってもジョブ取得は遅くならないこと。**
//
// 待っているのは marker への push で、Lua がジョブ投入時に push するので
// 即座に起きる。ここが崩れると、アイドルが長いほど最初の 1 件が遅れる。
func TestIdleBackoff_DoesNotDelayPickup(t *testing.T) {
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

	// 待ちが上限近くまで伸びるのを待つ。
	time.Sleep(8 * time.Second)

	enqueued := time.Now()
	_, err = queue.Add(context.Background(), testPayload{Inbox: "wake"})
	require.NoError(t, err)

	select {
	case at := <-got:
		elapsed := at.Sub(enqueued)
		assert.Lessf(t, elapsed, time.Second,
			"取得に %v かかった。marker で起きていない", elapsed)
		t.Logf("バックオフが伸びた状態から %v で取得", elapsed.Round(time.Millisecond))
	case <-time.After(15 * time.Second):
		t.Fatal("ジョブを取得できない")
	}
}

// **Stop は idlePollInterval に律速されないこと。**
//
// ctx キャンセルは発行済みの BZPOPMIN を中断できないので、marker を突いて
// 起こす必要がある。無いと停止に最大 interval かかる (実測: 8 秒設定で 7.78 秒)。
func TestStop_NotBoundedByIdleInterval(t *testing.T) {
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	const interval = 8 * time.Second
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(interval), mkq.WithConcurrency(4))
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond) // marker 待ちに入るのを待つ

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, worker.Stop(ctx))
	elapsed := time.Since(start)

	assert.Lessf(t, elapsed, interval/2,
		"Stop に %v かかった (interval=%v)。marker で起こせていない", elapsed, interval)
	t.Logf("interval=%v でも Stop %v", interval, elapsed.Round(time.Millisecond))
}
