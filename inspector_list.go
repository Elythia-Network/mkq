package mkq

import (
	"context"
	"fmt"
	"slices"

	"github.com/redis/go-redis/v9"

	"github.com/elythia-network/mkq/internal/lua"
)

// ListedJob bundles a typed Job with its post-processing JobState
// snapshot — the ListJobs caller usually wants both halves in a
// single round-trip.
type ListedJob[T any] struct {
	Job   *Job[T]
	State *JobState
}

// ListJobs returns the jobs sitting in `bucket` between [start, end]
// (inclusive, zero-based) at the moment of the call. The semantics
// mirror BullMQ's Queue.getJobs: pass `start=0, end=-1` to fetch the
// whole bucket, `start=0, end=N` for the first N+1 entries, and so
// on. asynq-style page/pageSize callers can shim with
// `(start = page*pageSize, end = start+pageSize-1)`.
//
// The bucket determines fetch semantics inside the lua:
//
//   - LIST-backed buckets (wait, paused, active) are read with LRANGE.
//     ascending=false returns them in LIST-native order, which for
//     BullMQ's LPUSH-backed lists is newest first.
//   - ZSET-backed buckets (delayed, prioritized, completed, failed)
//     read by score.
//
// Pass ascending=true to get the oldest-first / lowest-score-first
// view that admin UIs typically want; ascending=false returns the
// BullMQ default (newest first / highest score first). Ordering holds
// across pages, not just within one, so walking pages 0..n in
// ascending order walks the bucket oldest to newest.
//
// Each returned ListedJob carries both the typed Job (with the user
// payload decoded into T) and the JobState snapshot (terminal-state
// fields like FinishedOn / ReturnValue). For jobs still in flight
// JobState is non-nil but its time fields stay zero.
func (q *Queue[T]) ListJobs(ctx context.Context, bucket JobBucket, start, end int64, ascending bool) ([]ListedJob[T], error) {
	asc := "0"
	if ascending {
		asc = "1"
	}

	// **BullMQ 6 で paused リストは使われなくなった。** pause してもジョブは
	// wait に残るので、paused を要求されたら停止中は wait を読む。そうしないと
	// Counts が「停止中 5 件」と言っているのに一覧が空、という食い違いが出る。
	// 停止していなければ paused は空集合を返すのが正しい。
	//
	// 分岐は QueueCounts.Paused と同じ順序で判定すること。ずれると
	// 件数と一覧が食い違う。
	source := bucket
	if bucket == JobBucketPaused {
		// legacy paused リストが残っているなら、Counts と同じくそれを
		// paused として読む。空のときだけ v6 の意味論に落とす。
		legacy, err := q.client.rdb.LLen(ctx, q.keys.Paused()).Result()
		if err != nil {
			return nil, fmt.Errorf("mkq: LLEN paused: %w", err)
		}
		if legacy == 0 {
			paused, err := q.IsPaused(ctx)
			if err != nil {
				return nil, err
			}
			if !paused {
				return []ListedJob[T]{}, nil
			}
			source = JobBucketWait
		}
	}

	res, err := q.client.scripts.Run(
		ctx,
		lua.GetRanges,
		[]string{q.keys.Base()},
		fmt.Sprintf("%d", start),
		fmt.Sprintf("%d", end),
		asc,
		string(source),
	)
	if err != nil {
		return nil, fmt.Errorf("mkq: getRanges: %w", err)
	}

	// getRanges-1.lua は要求された bucket ごとに 1 エントリ返す。
	// ListJobs は常に 1 bucket しか渡さないので、外側の配列を剥がす。
	outer, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("mkq: getRanges: unexpected result type %T", res)
	}
	if len(outer) == 0 {
		// safe-range contract: 空でも nil ではなく empty slice を返す。
		return []ListedJob[T]{}, nil
	}
	ids, err := toStringSlice(outer[0])
	if err != nil {
		return nil, fmt.Errorf("mkq: getRanges: %w", err)
	}
	if len(ids) == 0 {
		return []ListedJob[T]{}, nil
	}
	// **LIST は窓を選ぶだけで並べ替えない。** `getRangeInList` は asc のとき
	// 負のインデックスへ読み替えて LRANGE する。これは「古い側から窓を取る」
	// 処理であって、取れた窓の中は LIST ネイティブ順 (新しい順) のまま。
	// bucket 全体を asc で取れば降順がそのまま返ってくる。
	//
	// BullMQ TS も同じ Lua を使い、同じ場所で反転している
	// (`src/classes/queue-getters.ts` の getRanges)。ここを省くと同じキューを
	// bull-board と並べたときに順序が食い違う。Lua は vendored なので触らない。
	if ascending && isListBucket(source) {
		slices.Reverse(ids)
	}

	return q.fetchJobs(ctx, ids)
}

// isListBucket reports whether a bucket is backed by a Redis LIST
// rather than a ZSET. getRanges-1.lua branches on the same three names.
func isListBucket(b JobBucket) bool {
	return b == JobBucketWait || b == JobBucketPaused || b == JobBucketActive
}

// fetchJobs HGETALLs every id in a pipeline (one round-trip per
// pipeline batch). Missing HASHes (e.g. job removed between getRanges
// and HGETALL) are skipped silently — the lua's snapshot is point-in-
// time and a follow-up reader can race with a delete.
func (q *Queue[T]) fetchJobs(ctx context.Context, ids []string) ([]ListedJob[T], error) {
	pipe := q.client.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, q.keys.Job(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("mkq: pipeline HGetAll: %w", err)
	}

	out := make([]ListedJob[T], 0, len(ids))
	for i, cmd := range cmds {
		hash, err := cmd.Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("mkq: HGetAll %s: %w", ids[i], err)
		}
		if len(hash) == 0 {
			continue
		}
		job, err := buildJob[T](ids[i], hash)
		if err != nil {
			return nil, fmt.Errorf("mkq: buildJob %s: %w", ids[i], err)
		}
		job.queue = q
		out = append(out, ListedJob[T]{Job: job, State: buildJobState(hash)})
	}
	return out, nil
}

// toStringSlice normalises go-redis's polymorphic Lua-array result
// (often []any of strings) into []string. Empty / nil inputs return
// nil so callers can range over the result safely.
func toStringSlice(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any, got %T", v)
	}
	out := make([]string, 0, len(arr))
	for i, raw := range arr {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("element %d not a string (%T)", i, raw)
		}
		out = append(out, s)
	}
	return out, nil
}
