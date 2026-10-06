package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// listIDs walks a bucket one page at a time and returns the ids in the
// order a caller paging through them would see.
func listIDs(t *testing.T, ctx context.Context, q *mkq.Queue[testPayload], b mkq.JobBucket, size int64, ascending bool) []string {
	t.Helper()
	var out []string
	for page := int64(0); ; page++ {
		start := page * size
		listed, err := q.ListJobs(ctx, b, start, start+size-1, ascending)
		require.NoError(t, err)
		if len(listed) == 0 {
			return out
		}
		for _, lj := range listed {
			out = append(out, lj.Job.ID)
		}
	}
}

// TestInspector_ListJobs_AscendingIsOldestFirst pins what the godoc
// promises and what BullMQ TS actually returns.
//
// **Lua は窓を選ぶだけで並べ替えない。** `getRangeInList` は asc のとき
// 負のインデックスへ読み替えて LRANGE するが、取れた窓の中は LIST ネイティブ
// 順 (新しい順) のまま。bucket 全体を asc で取ると降順がそのまま返ってくる。
// BullMQ TS は同じ Lua を使ったうえで取得後に反転しており
// (`queue-getters.ts` の getRanges)、ここを揃えないと同じキューを bull-board と
// 並べたときに順序が食い違う。
func TestInspector_ListJobs_AscendingIsOldestFirst(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added := make([]string, 0, 5)
	for i := range 5 {
		j, err := queue.Add(ctx, testPayload{Inbox: string(rune('a' + i))})
		require.NoError(t, err)
		added = append(added, j.ID)
	}

	whole, err := queue.ListJobs(ctx, mkq.JobBucketWait, 0, -1, true)
	require.NoError(t, err)
	wholeIDs := make([]string, 0, len(whole))
	for _, lj := range whole {
		wholeIDs = append(wholeIDs, lj.Job.ID)
	}
	assert.Equal(t, added, wholeIDs,
		"the whole bucket in ascending order must be oldest first")

	// ページを跨いでも通しで古い順。境界でひっくり返らないこと。
	assert.Equal(t, added, listIDs(t, ctx, queue, mkq.JobBucketWait, 2, true),
		"ascending order must hold across pages, not only within one")
}

// ascending=false must keep returning the lua-native order, which is
// what BullMQ defaults to and what existing callers already see.
func TestInspector_ListJobs_DescendingIsUnchanged(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added := make([]string, 0, 4)
	for i := range 4 {
		j, err := queue.Add(ctx, testPayload{Inbox: string(rune('a' + i))})
		require.NoError(t, err)
		added = append(added, j.ID)
	}

	want := make([]string, len(added))
	for i, id := range added {
		want[len(added)-1-i] = id
	}

	got, err := queue.ListJobs(ctx, mkq.JobBucketWait, 0, -1, false)
	require.NoError(t, err)
	gotIDs := make([]string, 0, len(got))
	for _, lj := range got {
		gotIDs = append(gotIDs, lj.Job.ID)
	}
	assert.Equal(t, want, gotIDs, "descending must stay newest first")
}

// ZSET-backed buckets are ordered by the lua's ZRANGE / ZREVRANGE, so
// the Go-side reversal must not touch them.
func TestInspector_ListJobs_ZSetBucketsAreNotReversed(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	added := make([]string, 0, 3)
	for i := range 3 {
		j, err := queue.Add(ctx, testPayload{Inbox: string(rune('a' + i))})
		require.NoError(t, err)
		added = append(added, j.ID)
		// completed の score は finishedOn (ミリ秒)。同じミリ秒に 2 件
		// 入ると順序が決まらないので、1 件ずつ間隔を空けて処理する。
		time.Sleep(5 * time.Millisecond)
	}

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		counts, err := queue.Counts(ctx, mkq.JobBucketCompleted)
		return err == nil && counts.Completed == 3
	})

	asc, err := queue.ListJobs(ctx, mkq.JobBucketCompleted, 0, -1, true)
	require.NoError(t, err)
	ascIDs := make([]string, 0, len(asc))
	for _, lj := range asc {
		ascIDs = append(ascIDs, lj.Job.ID)
	}
	assert.Equal(t, added, ascIDs, "ZRANGE already yields oldest first")
}

// The active bucket is a LIST too, and is the one an operator reads
// when asking "what is running right now". Its order must follow the
// same rule as wait.
//
// 期待値を「積んだ順」ではなく「Redis の LIST を反転したもの」に取るのは、
// concurrency > 1 だと dispatcher が同時に pop するので dequeue の順序が
// レースするため。ここで確かめたいのは順序の由来ではなく、asc が LIST
// ネイティブ順を反転することそのもの。
func TestInspector_ListJobs_ActiveBucketAscending(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const n = 3
	for i := range n {
		_, err := queue.Add(ctx, testPayload{Inbox: string(rune('a' + i))})
		require.NoError(t, err)
	}

	release := make(chan struct{})
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		<-release
		return nil, nil
	}, mkq.WithConcurrency(n), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer func() {
		close(release)
		_ = worker.Stop(context.Background())
	}()

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n2, err := rdb.LLen(ctx, base+"active").Result()
		return err == nil && n2 == n
	})

	native, err := rdb.LRange(ctx, base+"active", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, native, n)

	want := make([]string, n)
	for i, id := range native {
		want[n-1-i] = id
	}

	listed, err := queue.ListJobs(ctx, mkq.JobBucketActive, 0, -1, true)
	require.NoError(t, err)
	got := make([]string, 0, len(listed))
	for _, lj := range listed {
		got = append(got, lj.Job.ID)
	}
	assert.Equal(t, want, got, "ascending on the active LIST must reverse the native order")
}
