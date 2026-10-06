package mkq

import (
	"context"
	"fmt"

	"github.com/elythia-network/mkq/internal/lua"
)

// JobBucket identifies a state-key bucket the BullMQ wire format uses
// to partition jobs (waiting / active / etc.). The string values match
// BullMQ verbatim — they're sent into getCounts-1.lua and
// getRanges-1.lua as ARGV, where they're concatenated onto the queue
// prefix to derive the actual Redis key.
//
// The deliberate design choice is to use a string type rather than an
// int enum so foreign callers (admin tooling, log lines) can compare
// by value without reaching into mkq for a stringer.
type JobBucket string

// JobBucket constants align with BullMQ's vendored Lua expectations.
// "wait" / "paused" / "active" are LIST-backed; the rest are ZSET-
// backed. The lua scripts handle the LIST vs ZSET branch internally,
// so callers can mix bucket types in a single Counts / ListJobs call.
const (
	JobBucketWait        JobBucket = "wait"
	JobBucketActive      JobBucket = "active"
	JobBucketDelayed     JobBucket = "delayed"
	JobBucketPrioritized JobBucket = "prioritized"
	JobBucketCompleted   JobBucket = "completed"
	JobBucketFailed      JobBucket = "failed"
	JobBucketPaused      JobBucket = "paused"
)

// allBuckets lists every bucket Counts iterates when the caller
// supplies no explicit set. Matches the order the QueueCounts struct
// exposes them.
var allBuckets = []JobBucket{
	JobBucketWait, JobBucketActive, JobBucketDelayed, JobBucketPrioritized,
	JobBucketCompleted, JobBucketFailed, JobBucketPaused,
}

// QueueCounts is the per-state job count snapshot returned by
// Queue[T].Counts. Fields not requested in the Counts call stay at
// their zero value.
type QueueCounts struct {
	Wait        int64
	Active      int64
	Delayed     int64
	Prioritized int64
	Completed   int64
	Failed      int64
	// Paused is how many jobs are held back by a pause, and 0 when the
	// queue is running. ListJobs(JobBucketPaused) returns exactly these
	// jobs.
	//
	// **BullMQ 6 以降、paused は状態ではなくフラグ。** ジョブは wait に
	// 残ったままなので、ここに載るのは「停止中の wait の件数」になる。
	// 同じジョブは Wait にも数えられる。
	//
	// 例外は BullMQ 5 が pause したまま残していった legacy paused リスト。
	// 空でなければその件数をそのまま返す (この場合は Wait と重複しない)。
	// Resume が吸い出せば消え、以降は上の v6 の意味論だけになる。
	Paused int64
}

// Counts returns the number of jobs currently sitting in each of the
// requested buckets. Calling Counts with no buckets returns counts
// for every bucket (matching the BullMQ Queue.getJobCounts() default).
//
// The lua collapses the per-bucket LLEN/ZCARD calls into one round
// trip; pure-Go callers that only need one count are still better
// off via the dedicated helper rather than repeated single-state
// Counts calls.
//
// Requesting JobBucketPaused costs one extra round trip (HEXISTS on
// `meta`), because under BullMQ 6 the `paused` list length no longer
// answers the question on its own. Omit that bucket on hot paths that
// do not need it.
func (q *Queue[T]) Counts(ctx context.Context, buckets ...JobBucket) (QueueCounts, error) {
	if len(buckets) == 0 {
		buckets = allBuckets
	}

	// **BullMQ 6 で paused リストは使われなくなった。** pause してもジョブは
	// wait に残り、gate は meta.paused のフラグだけが持つ。そのまま数えると
	// Paused は常に 0 で、admin UI の「停止中に何件溜まっているか」に
	// 答えられない。停止中は wait の件数をそこに載せる。
	query := buckets
	wantPaused := containsBucket(buckets, JobBucketPaused)
	wantWait := containsBucket(buckets, JobBucketWait)
	if wantPaused && !wantWait {
		query = append(append(make([]JobBucket, 0, len(buckets)+1), buckets...), JobBucketWait)
	}

	args := make([]any, 0, len(query))
	for _, b := range query {
		args = append(args, string(b))
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.GetCounts,
		[]string{q.keys.Base()},
		args...,
	)
	if err != nil {
		return QueueCounts{}, fmt.Errorf("mkq: getCounts: %w", err)
	}
	arr, ok := res.([]any)
	if !ok {
		return QueueCounts{}, fmt.Errorf("mkq: getCounts: unexpected result type %T", res)
	}
	if len(arr) != len(query) {
		return QueueCounts{}, fmt.Errorf("mkq: getCounts: result length %d != buckets %d", len(arr), len(query))
	}

	var out QueueCounts
	var waitCount int64
	for i, raw := range arr {
		n, _ := toInt64(raw)
		if query[i] == JobBucketWait {
			waitCount = n
			if !wantWait {
				// 呼び出し側が wait を求めていないなら、Paused の算出に
				// 使うだけで結果には載せない。
				continue
			}
		}
		assignBucket(&out, query[i], n)
	}

	if wantPaused {
		// ここまでで out.Paused には legacy paused リストの LLEN が入って
		// いる。BullMQ 5 が pause したまま残していったジョブがそこにいる
		// 場合は、それをそのまま paused として報告する (v5 のレイアウトを
		// v5 の意味で読む)。Resume が吸い出せば 0 になり、以降は v6 の
		// 意味論に切り替わる。
		if out.Paused == 0 {
			paused, err := q.IsPaused(ctx)
			if err != nil {
				return QueueCounts{}, err
			}
			if paused {
				out.Paused = waitCount
			}
		}
	}
	return out, nil
}

func containsBucket(buckets []JobBucket, want JobBucket) bool {
	for _, b := range buckets {
		if b == want {
			return true
		}
	}
	return false
}

// assignBucket dispatches a per-bucket count into the corresponding
// QueueCounts field. Unknown buckets are dropped silently — they
// never reach this path because allBuckets / caller-supplied values
// are validated by the lua's branch matching, but the default case
// keeps the function safe against future bucket additions.
func assignBucket(c *QueueCounts, b JobBucket, n int64) {
	switch b {
	case JobBucketWait:
		c.Wait = n
	case JobBucketActive:
		c.Active = n
	case JobBucketDelayed:
		c.Delayed = n
	case JobBucketPrioritized:
		c.Prioritized = n
	case JobBucketCompleted:
		c.Completed = n
	case JobBucketFailed:
		c.Failed = n
	case JobBucketPaused:
		c.Paused = n
	}
}
