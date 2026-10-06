//go:build interop

// Wire-format interop coverage. Each subtest pins one BullMQ-shaped
// wire surface mkq writes — Job HASH fields, priority/delay/lifo
// queues, retention shape, custom name — and asserts BullMQ TS reads
// it correctly via Job.fromId / Queue.getJobs / Queue.getJobCounts.
//
// Direction is mkq→BullMQ for every subtest in this file; the
// inverse direction (BullMQ-Add → mkq-Get) is covered in
// interop_test.go's existing TestInterop_BullMQAddMkqProcess.

package interop_test

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInterop_Wire_HashParity confirms the Job HASH fields mkq writes
// (name, data, opts, timestamp) round-trip through BullMQ TS's
// Job.fromId. Without this, a future opts JSON-encoding tweak in
// proto/opts.go could silently break BullMQ's reader.
func TestInterop_Wire_HashParity(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "x", Body: "hash-parity"},
		mkq.WithAttempts(3),
	)
	require.NoError(t, err)

	var got struct {
		ID        string         `json:"id"`
		Name      string         `json:"name"`
		Data      interopPayload `json:"data"`
		Opts      map[string]any `json:"opts"`
		Timestamp int64          `json:"timestamp"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJob", job.ID)
	assert.Equal(t, job.ID, got.ID)
	assert.Equal(t, queueName, got.Name)
	assert.Equal(t, "x", got.Data.Inbox)
	assert.Equal(t, "hash-parity", got.Data.Body)
	assert.EqualValues(t, 3, got.Opts["attempts"])
	assert.NotZero(t, got.Timestamp)
}

// TestInterop_Wire_Priority confirms BullMQ TS dequeues mkq-added
// priority jobs in priority order: lower numeric priority runs first
// (matching BullMQ's documented semantics).
func TestInterop_Wire_Priority(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	low, err := queue.Add(ctx, interopPayload{Inbox: "low"}, mkq.WithPriority(10))
	require.NoError(t, err)
	high, err := queue.Add(ctx, interopPayload{Inbox: "high"}, mkq.WithPriority(1))
	require.NoError(t, err)

	startNodeWorker(t, prefix, queueName)

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, base+"completed").Result()
		return n >= 2
	})

	// **完了時刻では判定できない。** completed ZSET のスコアはミリ秒なので、
	// 2 件が同じミリ秒に収まると同値になり、順序の主張が立たない (実際に
	// 断続的に落ちていた)。優先度が保証しているのは「先に取り出されること」
	// なので、挿入順が保たれる events stream の active を見る。
	entries, err := rdb.XRange(ctx, base+"events", "-", "+").Result()
	require.NoError(t, err)

	var activeOrder []string
	for _, e := range entries {
		if e.Values["event"] != "active" {
			continue
		}
		if id, ok := e.Values["jobId"].(string); ok {
			activeOrder = append(activeOrder, id)
		}
	}

	highAt := slices.Index(activeOrder, high.ID)
	lowAt := slices.Index(activeOrder, low.ID)
	require.GreaterOrEqual(t, highAt, 0, "no active event for the high-priority job: %v", activeOrder)
	require.GreaterOrEqual(t, lowAt, 0, "no active event for the low-priority job: %v", activeOrder)
	assert.Less(t, highAt, lowAt,
		"priority=1 (high) must be dequeued before priority=10 (low), got order %v", activeOrder)
}

// TestInterop_Wire_DelayPickup confirms BullMQ TS Worker honours
// mkq's delayed ZSET score: a WithDelay(t) job is not picked up
// before t and is then promoted normally.
func TestInterop_Wire_DelayPickup(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const delay = 600 * time.Millisecond
	addedAt := time.Now()
	job, err := queue.Add(ctx, interopPayload{Inbox: "delayed"}, mkq.WithDelay(delay))
	require.NoError(t, err)

	startNodeWorker(t, prefix, queueName)

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	finishedScore, err := rdb.ZScore(ctx, base+"completed", job.ID).Result()
	require.NoError(t, err)
	finishedAt := time.UnixMilli(int64(finishedScore))
	assert.GreaterOrEqual(t, finishedAt.Sub(addedAt), delay-100*time.Millisecond,
		"delayed job must not be picked up before its target time")
}

// TestInterop_Wire_RetentionCountAge confirms BullMQ TS observes
// mkq's WithKeepCompleted(N)+WithKeepCompletedAge(...) settings as
// the canonical {count, age} shape in opts.removeOnComplete.
func TestInterop_Wire_RetentionCountAge(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "retention"},
		mkq.WithKeepCompleted(3),
		mkq.WithKeepCompletedAge(60*time.Second),
	)
	require.NoError(t, err)

	var got struct {
		Opts struct {
			RemoveOnComplete struct {
				Count int `json:"count"`
				Age   int `json:"age"`
			} `json:"removeOnComplete"`
		} `json:"opts"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJob", job.ID)
	assert.Equal(t, 3, got.Opts.RemoveOnComplete.Count, "count field must round-trip")
	assert.Equal(t, 60, got.Opts.RemoveOnComplete.Age, "age field must round-trip")
}

// TestInterop_Wire_JobName confirms BullMQ TS reads the per-Add
// Job.name override mkq sets via WithJobName. mk-go's MkqDriver
// adapter dispatches on this field, so the round-trip must stay tight.
func TestInterop_Wire_JobName(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{}, mkq.WithJobName("system:retention"))
	require.NoError(t, err)

	var got struct {
		Name string `json:"name"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJob", job.ID)
	assert.Equal(t, "system:retention", got.Name)
}

// TestInterop_Wire_ListJobs confirms BullMQ TS Queue.getJobs returns
// the same id set mkq's Queue.ListJobs would, for the same wait
// bucket. Range bounds use BullMQ semantics (start, end inclusive).
func TestInterop_Wire_ListJobs(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const seed = 5
	addedIDs := make([]string, seed)
	for i := range seed {
		j, err := queue.Add(ctx, interopPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
		addedIDs[i] = j.ID
	}

	var got struct {
		IDs []string `json:"ids"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJobs", "wait", "0", "-1")
	want := append([]string(nil), addedIDs...)
	sort.Strings(want)
	gotIDs := append([]string(nil), got.IDs...)
	sort.Strings(gotIDs)
	assert.Equal(t, want, gotIDs, "BullMQ Queue.getJobs must return every mkq-added job")
}

// TestInterop_Wire_Counts confirms BullMQ TS Queue.getJobCounts
// matches mkq's Queue.Counts on the same backing keys: 3 wait + 2
// delayed should report 3/2 from both libraries.
func TestInterop_Wire_Counts(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "wire"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range 3 {
		_, err := queue.Add(ctx, interopPayload{})
		require.NoError(t, err)
	}
	for range 2 {
		_, err := queue.Add(ctx, interopPayload{}, mkq.WithDelay(10*time.Minute))
		require.NoError(t, err)
	}

	mkqCounts, err := queue.Counts(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, mkqCounts.Wait)
	require.EqualValues(t, 2, mkqCounts.Delayed)

	var bullCounts struct {
		Counts struct {
			Wait    int64 `json:"wait"`
			Delayed int64 `json:"delayed"`
		} `json:"counts"`
	}
	runNodeInspector(t, prefix, queueName, &bullCounts, "counts", "wait", "delayed")
	assert.EqualValues(t, 3, bullCounts.Counts.Wait)
	assert.EqualValues(t, 2, bullCounts.Counts.Delayed)
}

// _ ensures json import survives if a subtest is later commented out.
var _ = json.Unmarshal
