package mkq_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestJobMutation_UpdateProgress_EmitsEvent verifies the wire round
// trip: handler writes progress via the Job back-pointer; the BullMQ
// `progress` HASH field reflects the update and a `progress` event
// is XADDed to the events stream so a QueueEvents subscriber observes
// it in real time.
func TestJobMutation_UpdateProgress_EmitsEvent(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qe := mkq.NewQueueEvents(queue)
	t.Cleanup(qe.Close)

	gotProgress := make(chan mkq.Event, 4)
	subDone := make(chan error, 1)
	go func() {
		subDone <- qe.Subscribe(ctx, func(e mkq.Event) error {
			if _, ok := e.(mkq.ProgressEvent); ok {
				select {
				case gotProgress <- e:
				default:
				}
			}
			return nil
		})
	}()
	// QueueEvents は "$" 起点で購読する。テストの handler が走る前に
	// 確実に subscribe を確立させるため、軽くスリープ。
	time.Sleep(50 * time.Millisecond)

	addedJob, err := queue.Add(ctx, testPayload{Inbox: "https://x"})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(ctx context.Context, j *mkq.Job[testPayload]) (any, error) {
		assert.NoError(t, j.UpdateProgress(ctx, 0.5))
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", addedJob.ID).Result()
		return v > 0
	})

	progress, err := rdb.HGet(ctx, base+addedJob.ID, "progress").Result()
	require.NoError(t, err)
	assert.Equal(t, "0.5", progress, "HASH `progress` field must reflect the update")

	select {
	case ev := <-gotProgress:
		pe := ev.(mkq.ProgressEvent)
		assert.Equal(t, addedJob.ID, pe.JobID)
		assert.JSONEq(t, "0.5", string(pe.Data))
	case <-time.After(2 * time.Second):
		t.Fatal("did not observe progress event within 2s")
	}

	qe.Close()
	<-subDone
}

// TestJobMutation_UpdateData_ReplacesField confirms UpdateData
// rewrites the BullMQ `data` HASH field with the new JSON. A retry
// triggered after the handler returns sees the new payload via
// buildJob.
func TestJobMutation_UpdateData_ReplacesField(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, testPayload{Inbox: "old"})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(ctx context.Context, j *mkq.Job[testPayload]) (any, error) {
		assert.NoError(t, j.UpdateData(ctx, testPayload{Inbox: "new", Body: "after"}))
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", addedJob.ID).Result()
		return v > 0
	})

	raw, err := rdb.HGet(ctx, base+addedJob.ID, "data").Result()
	require.NoError(t, err)
	var got testPayload
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Equal(t, "new", got.Inbox)
	assert.Equal(t, "after", got.Body)
}

// TestJobMutation_Log_AppendsToList exercises Log append semantics.
// Two calls produce two LIST entries in submission order; bull-board
// renders them as the job's log tab.
func TestJobMutation_Log_AppendsToList(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(ctx context.Context, j *mkq.Job[testPayload]) (any, error) {
		assert.NoError(t, j.Log(ctx, "first"))
		assert.NoError(t, j.Log(ctx, "second"))
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", addedJob.ID).Result()
		return v > 0
	})

	logs, err := rdb.LRange(ctx, base+addedJob.ID+":logs", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second"}, logs)
}

// TestJobMutation_OutOfBand_QueueMethods covers the parallel admin
// path: callers without a Job handle (e.g. a reporter goroutine
// holding only the Queue + jobID) can still mutate via
// Queue.UpdateJobProgress / UpdateJobData / AppendJobLog.
func TestJobMutation_OutOfBand_QueueMethods(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "init"})
	require.NoError(t, err)

	// Job がまだ wait 状態でも HASH は存在するので、out-of-band 経路で
	// 全 3 method が成功することを確認。
	require.NoError(t, queue.UpdateJobProgress(ctx, job.ID, map[string]any{"phase": "x", "pct": 42}))
	require.NoError(t, queue.UpdateJobData(ctx, job.ID, testPayload{Inbox: "rewritten"}))
	require.NoError(t, queue.AppendJobLog(ctx, job.ID, "out-of-band note"))

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	progress, err := rdb.HGet(ctx, base+job.ID, "progress").Result()
	require.NoError(t, err)
	assert.JSONEq(t, `{"phase":"x","pct":42}`, progress)

	dataRaw, err := rdb.HGet(ctx, base+job.ID, "data").Result()
	require.NoError(t, err)
	var data testPayload
	require.NoError(t, json.Unmarshal([]byte(dataRaw), &data))
	assert.Equal(t, "rewritten", data.Inbox)

	logs, err := rdb.LRange(ctx, base+job.ID+":logs", 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{"out-of-band note"}, logs)
}

// TestJobMutation_MissingJob_ReturnsErrJobNotFound pins the wire
// contract: when the BullMQ HASH is gone (manually deleted, retention
// cleared), the lua scripts return -1 and mkq surfaces ErrJobNotFound.
func TestJobMutation_MissingJob_ReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const missing = "does-not-exist"

	t.Run("UpdateJobProgress", func(t *testing.T) {
		err := queue.UpdateJobProgress(ctx, missing, 0.1)
		assert.ErrorIs(t, err, mkq.ErrJobNotFound)
	})
	t.Run("UpdateJobData", func(t *testing.T) {
		err := queue.UpdateJobData(ctx, missing, testPayload{})
		assert.ErrorIs(t, err, mkq.ErrJobNotFound)
	})
	t.Run("AppendJobLog", func(t *testing.T) {
		err := queue.AppendJobLog(ctx, missing, "x")
		assert.ErrorIs(t, err, mkq.ErrJobNotFound)
	})
}

// TestJobMutation_DetachedJob_ReturnsErrJobDetached pins the safety
// behavior for synthetic Jobs (e.g. zero-valued in tests, or built
// outside the mkq API): mutation methods return ErrJobDetached
// instead of panicking on a nil queue back-pointer.
func TestJobMutation_DetachedJob_ReturnsErrJobDetached(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var j mkq.Job[testPayload]
	assert.True(t, errors.Is(j.UpdateProgress(ctx, 1), mkq.ErrJobDetached))
	assert.True(t, errors.Is(j.UpdateData(ctx, testPayload{}), mkq.ErrJobDetached))
	assert.True(t, errors.Is(j.Log(ctx, "x"), mkq.ErrJobDetached))
}
