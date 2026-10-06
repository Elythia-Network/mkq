package mkq_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// jobOptsOf reads the `opts` field a job HASH carries, which is what
// moveToFinished consults for removeOnComplete / removeOnFail.
func jobOptsOf(t *testing.T, ctx context.Context, prefix, queue, jobID string) map[string]any {
	t.Helper()
	raw, err := rawClient(t).HGet(ctx, prefix+":"+queue+":"+jobID, "opts").Result()
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	return out
}

// TestSchedule_RetentionReachesTemplateAndFirstIteration pins where the
// retention has to land for it to work at all.
//
// scheduler HASH の `opts` は、BullMQ TS の Worker が再スケジュールする
// ときに読む template。per-iteration の job opts は mkq / BullMQ どちらが
// 積んでも各 job HASH に書かれる。**両方に載っていないと、どちらかの経路で
// retention が落ちる。**
func TestSchedule_RetentionReachesTemplateAndFirstIteration(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, "ticker", time.Hour,
		testPayload{Inbox: "x"},
		mkq.WithScheduleKeepCompletedAge(7*24*time.Hour),
		mkq.WithScheduleKeepFailed(50),
	))

	rdb := rawClient(t)

	// 1) scheduler HASH の template
	tmplRaw, err := rdb.HGet(ctx, prefix+":tick:repeat:ticker", "opts").Result()
	require.NoError(t, err, "the scheduler HASH must carry an opts template")
	var tmpl map[string]any
	require.NoError(t, json.Unmarshal([]byte(tmplRaw), &tmpl))
	assert.Equal(t, map[string]any{"age": float64(7 * 24 * 3600)}, tmpl["removeOnComplete"],
		"age-only retention takes BullMQ's object form")
	assert.EqualValues(t, 50, tmpl["removeOnFail"],
		"count-only retention takes BullMQ's number shorthand")

	// 2) 1 本目の iteration の job opts。wait は LIST。
	ids, err := rdb.LRange(ctx, prefix+":tick:wait", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, ids, 1, "UpsertScheduleEvery queues the first iteration")

	opts := jobOptsOf(t, ctx, prefix, "tick", ids[0])
	assert.Equal(t, map[string]any{"age": float64(7 * 24 * 3600)}, opts["removeOnComplete"])
	assert.EqualValues(t, 50, opts["removeOnFail"])
	require.Contains(t, opts, "repeat",
		"the repeat block must survive alongside the template opts")
}

// TestSchedule_RetentionSurvivesReschedule is the regression the worker
// change fixes.
//
// **1 本目だけ通っても意味がない。** 2 本目以降は worker が scheduler HASH を
// 読んで per-iteration opts を組み直すので、そこで template を拾わないと
// retention は最初の 1 回しか載らない — つまり定期ジョブはやはり溜まり続ける。
func TestSchedule_RetentionSurvivesReschedule(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// **間隔は長く取る。** `UpsertScheduleEvery` は 1 本目を即座に wait へ
	// 積むので、worker が 1 本処理すれば 2 本目が delayed に入る。短い間隔
	// (150ms) にすると 2 本目がすぐ満期になって wait へ昇格し、delayed を
	// 見たときには空、ということが起きる (CI で実測)。長くしても 1 本目は
	// 即時なので、テストが遅くなることはない。
	require.NoError(t, queue.UpsertScheduleEvery(ctx, "ticker", time.Hour,
		testPayload{Inbox: "x"},
		mkq.WithScheduleKeepCompletedAge(7*24*time.Hour),
	))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		seen.Add(1)
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	// 1 本目 (Upsert が積んだもの) を処理させ、worker が 2 本目を delayed へ
	// 積むまで待つ。
	var ids []string
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		if seen.Load() < 1 {
			return false
		}
		got, err := rdb.ZRange(ctx, prefix+":tick:delayed", 0, -1).Result()
		if err != nil || len(got) == 0 {
			return false
		}
		ids = got
		return true
	})
	require.NotEmpty(t, ids, "the worker must have queued the next iteration")
	require.True(t, strings.HasPrefix(ids[0], "repeat:ticker:"))

	opts := jobOptsOf(t, ctx, prefix, "tick", ids[0])
	assert.Equal(t, map[string]any{"age": float64(7 * 24 * 3600)}, opts["removeOnComplete"],
		"an iteration the worker scheduled must inherit the template retention")
}

// The point of all this: the completed ZSET stops growing.
func TestSchedule_RetentionTrimsCompleted(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, "ticker", 100*time.Millisecond,
		testPayload{Inbox: "x"},
		mkq.WithScheduleKeepCompleted(1),
	))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		seen.Add(1)
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool { return seen.Load() >= 4 })

	counts, err := queue.Counts(ctx, mkq.JobBucketCompleted)
	require.NoError(t, err)
	assert.LessOrEqual(t, counts.Completed, int64(1),
		"keep-1 must bound the completed ZSET no matter how many iterations ran (saw %d)", seen.Load())
}

// Without retention the records pile up, which is the state the option
// exists to fix. Pinning it keeps the test above honest: if retention
// silently stopped applying, this one would still pass and that one
// would fail.
func TestSchedule_WithoutRetentionCompletedAccumulates(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, "ticker", 100*time.Millisecond,
		testPayload{Inbox: "x"}))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		seen.Add(1)
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		counts, err := queue.Counts(ctx, mkq.JobBucketCompleted)
		return err == nil && counts.Completed >= 3
	})

	counts, err := queue.Counts(ctx, mkq.JobBucketCompleted)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, counts.Completed, int64(3),
		"no retention means nothing is trimmed")
}

// count と age は独立に足せる。順序を変えても片方が消えないこと。
//
// BullMQ は両方あるとき `{count, age}` のオブジェクト形で持つ。片方だけの
// ときは age がオブジェクト形、count が数値 shorthand。
func TestSchedule_RetentionCountAndAgeCombine(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, "a", time.Hour, testPayload{},
		mkq.WithScheduleKeepCompleted(10),
		mkq.WithScheduleKeepCompletedAge(time.Hour),
	))
	require.NoError(t, queue.UpsertScheduleEvery(ctx, "b", time.Hour, testPayload{},
		mkq.WithScheduleKeepCompletedAge(time.Hour),
		mkq.WithScheduleKeepCompleted(10),
	))

	rdb := rawClient(t)
	want := map[string]any{"count": float64(10), "age": float64(3600)}
	for _, id := range []string{"a", "b"} {
		raw, err := rdb.HGet(ctx, prefix+":tick:repeat:"+id, "opts").Result()
		require.NoError(t, err)
		var tmpl map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &tmpl))
		assert.Equal(t, want, tmpl["removeOnComplete"],
			"schedule %q: count and age must both survive regardless of option order", id)
	}
}
