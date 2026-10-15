package mkq_test

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// observeStalledScans polls the queue's stalled-check key and returns the
// distinct scan timestamps (ARGV[3] that moveStalledJobsToWait stores in
// the key) seen within window, in ascending order.
//
// The key holds the timestamp of the scan that set it and lives for
// roughly one stalled interval, so a poll period far below the interval
// observes every scan.
func observeStalledScans(t *testing.T, rdb *redis.Client, key string, window time.Duration) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()

	seen := map[int64]struct{}{}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		v, err := rdb.Get(ctx, key).Result()
		switch {
		case err == nil:
			ts, perr := strconv.ParseInt(v, 10, 64)
			require.NoError(t, perr, "stalled-check value %q", v)
			seen[ts] = struct{}{}
		case errors.Is(err, redis.Nil), ctx.Err() != nil:
		default:
			require.NoError(t, err)
		}
		select {
		case <-ctx.Done():
			out := make([]int64, 0, len(seen))
			for ts := range seen {
				out = append(out, ts)
			}
			sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
			return out
		case <-tick.C:
		}
	}
}

// scanGaps returns the intervals between consecutive scans.
func scanGaps(ts []int64) []time.Duration {
	gaps := make([]time.Duration, 0, len(ts))
	for i := 1; i < len(ts); i++ {
		gaps = append(gaps, time.Duration(ts[i]-ts[i-1])*time.Millisecond)
	}
	return gaps
}

// TestWorker_StalledCheck_RunsEveryInterval pins that the stalled-jobs
// scan actually runs once per configured interval (#136). Before the fix
// the ticker fired at the same period as the stalled-check key's TTL, so
// most ticks found the key still alive and the scan ran only about every
// second interval.
//
// Deliberately not t.Parallel: the assertions are about timing, and
// running alone keeps scheduler jitter from the rest of the suite out.
func TestWorker_StalledCheck_RunsEveryInterval(t *testing.T) {
	const (
		interval = 100 * time.Millisecond
		window   = 3 * time.Second
	)
	// 理想は window / interval = 30 回。旧実装は (worker 1 つで) 16〜17 回
	// (空振り 12〜13 回)、期限を周期と同じにしたまま timer だけ張り直す形
	// では 23〜27 回 (空振り 3〜7 回) だった。-race や負荷の高い CI でも
	// 揺れないよう、回数の下限は理想の 8 割にし、空振り (周期の 1.5 倍を
	// 超えた間隔) は 1 回まで許す。
	ideal := int(window / interval)
	minScans := ideal * 8 / 10
	const maxSkipped = 1

	for _, tc := range []struct {
		name    string
		workers int
	}{
		{name: "single worker", workers: 1},
		// このケースは、キーで走査が 1 本にまとまっているか (上限) を見る
		// ためのもの。#136 の回帰 (空振り) は worker 1 つのケースで検出する。
		{name: "four workers share the scan", workers: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := uniquePrefix(t)
			c := newClient(t, prefix)
			queue := mkq.Define[testPayload](c, "deliver")
			noop := func(context.Context, *mkq.Job[testPayload]) (any, error) { return nil, nil }

			for i := 0; i < tc.workers; i++ {
				// 起動をずらして、各 worker の走査の位相を周期の中で散らす。
				// 同時に起動すると全員が同じ位相のまま張り直すので、キーで
				// まとめなくても走査が増えず、下の上限の検査が効かない。
				if i > 0 {
					time.Sleep(interval / time.Duration(tc.workers))
				}
				w, err := mkq.Process(queue, noop,
					mkq.WithStalledInterval(interval),
					mkq.WithIdlePollInterval(time.Second),
				)
				require.NoError(t, err)
				t.Cleanup(func() { stopWorker(t, w) })
			}

			scans := observeStalledScans(t, rawClient(t), prefix+":deliver:stalled-check", window)
			skipped := 0
			for _, g := range scanGaps(scans) {
				if g > interval*3/2 {
					skipped++
				}
			}
			t.Logf("scans=%d (ideal %d) skipped=%d gaps=%v", len(scans), ideal, skipped, scanGaps(scans))

			assert.GreaterOrEqual(t, len(scans), minScans,
				"stalled-check must run about once per interval")
			assert.LessOrEqual(t, skipped, maxSkipped,
				"scans must not skip an interval")
			// 複数 worker でも stalled-check キーが走査を 1 本にまとめる。
			// 周期より大幅に多く走ったら、キーによる直列化が壊れている。
			assert.LessOrEqual(t, len(scans), ideal*13/10+1,
				"concurrent workers must still serialise on the stalled-check key")
		})
	}

	// BullMQ と同じく、起動直後にも 1 回走査する。周期を長くしておき、
	// 1 周期を待たずにキーが現れることを見る。
	t.Run("scans once at start", func(t *testing.T) {
		prefix := uniquePrefix(t)
		c := newClient(t, prefix)
		queue := mkq.Define[testPayload](c, "deliver")
		noop := func(context.Context, *mkq.Job[testPayload]) (any, error) { return nil, nil }

		w, err := mkq.Process(queue, noop,
			mkq.WithStalledInterval(10*time.Second),
			mkq.WithIdlePollInterval(time.Second),
		)
		require.NoError(t, err)
		t.Cleanup(func() { stopWorker(t, w) })

		rdb := rawClient(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		waitFor(t, ctx, 10*time.Millisecond, func() bool {
			n, _ := rdb.Exists(ctx, prefix+":deliver:stalled-check").Result()
			return n == 1
		})
	})
}
