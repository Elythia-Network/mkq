//go:build interop

// Mutation + admin interop coverage. Each subtest runs an mkq-side
// state-change (UpdateProgress, UpdateData, Log, RemoveJob,
// PromoteJob, RetryJob, DrainPending) and asserts BullMQ TS observes
// the outcome via Job / Queue accessors. Direction is mkq→BullMQ.

package interop_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInterop_Mutation_Progress: mkq writes progress; BullMQ TS
// Job.fromId returns the same value via job.progress.
func TestInterop_Mutation_Progress(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "x"})
	require.NoError(t, err)
	require.NoError(t, queue.UpdateJobProgress(ctx, job.ID, 0.42))

	var got struct {
		Progress float64 `json:"progress"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJob", job.ID)
	assert.InDelta(t, 0.42, got.Progress, 0.0001)
}

// TestInterop_Mutation_Data: mkq rewrites data; BullMQ TS reads the
// new payload via job.data.
func TestInterop_Mutation_Data(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "old"})
	require.NoError(t, err)
	require.NoError(t, queue.UpdateJobData(ctx, job.ID, interopPayload{Inbox: "new", Body: "rewritten"}))

	var got struct {
		Data interopPayload `json:"data"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJob", job.ID)
	assert.Equal(t, "new", got.Data.Inbox)
	assert.Equal(t, "rewritten", got.Data.Body)
}

// TestInterop_Mutation_Log: mkq appends two log lines; BullMQ TS
// Queue.getJobLogs returns them in submission order.
func TestInterop_Mutation_Log(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{})
	require.NoError(t, err)
	require.NoError(t, queue.AppendJobLog(ctx, job.ID, "first"))
	require.NoError(t, queue.AppendJobLog(ctx, job.ID, "second"))

	var got struct {
		Logs  []string `json:"logs"`
		Count int      `json:"count"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getJobLogs", job.ID)
	assert.Equal(t, []string{"first", "second"}, got.Logs)
}

// TestInterop_Mutation_RemoveJob: mkq removes a wait job; BullMQ TS
// Queue.getJob returns null (= state "missing" in the inspector
// helper).
func TestInterop_Mutation_RemoveJob(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "doomed"})
	require.NoError(t, err)
	require.NoError(t, queue.RemoveJob(ctx, job.ID))

	var got struct {
		State string `json:"state"`
	}
	runNodeInspector(t, prefix, queueName, &got, "getState", job.ID)
	// BullMQ TS Job.getState returns "unknown" when the HASH is gone.
	assert.Contains(t, []string{"missing", "unknown"}, got.State)
}

// TestInterop_Mutation_PromoteJob: mkq promotes a delayed job;
// BullMQ TS observes the new state as "waiting".
func TestInterop_Mutation_PromoteJob(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "promoted"}, mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	var before struct {
		State string `json:"state"`
	}
	runNodeInspector(t, prefix, queueName, &before, "getState", job.ID)
	require.Equal(t, "delayed", before.State)

	require.NoError(t, queue.PromoteJob(ctx, job.ID))

	var after struct {
		State string `json:"state"`
	}
	runNodeInspector(t, prefix, queueName, &after, "getState", job.ID)
	assert.Equal(t, "waiting", after.State, "BullMQ Job.getState must reflect mkq's promote")
}

// TestInterop_Mutation_RetryJob exercises the failed→wait state
// transition mkq's RetryJob triggers, with BullMQ TS Job.getState
// confirming the new state. We deliberately keep the entire job
// lifecycle on the mkq side (failing handler → RetryJob) so the
// observable invariant is "BullMQ reads the wire state mkq mutated"
// rather than "two foreign workers race over the same job."
//
// Phase 1: mkq processor fails the job once (no WithAttempts → one
// shot to failed).
// Phase 2: BullMQ TS Job.getState confirms "failed".
// Phase 3: mkq RetryJob re-enqueues to wait.
// Phase 4: BullMQ TS Job.getState confirms "waiting".
func TestInterop_Mutation_RetryJob(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, interopPayload{Inbox: "retry-target"})
	require.NoError(t, err)

	// Phase 1: drive to failed via an mkq handler that always errors.
	failed := make(chan struct{})
	w1, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[interopPayload]) (any, error) {
		select {
		case failed <- struct{}{}:
		default:
		}
		return nil, assertionErr("intentional fail")
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	<-failed
	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", addedJob.ID).Result()
		return v > 0
	})
	require.NoError(t, w1.Stop(context.Background()))

	// Phase 2: BullMQ confirms failed.
	var beforeRetry struct {
		State string `json:"state"`
	}
	runNodeInspector(t, prefix, queueName, &beforeRetry, "getState", addedJob.ID)
	require.Equal(t, "failed", beforeRetry.State)

	// Phase 3: RetryJob re-enqueues.
	require.NoError(t, queue.RetryJob(ctx, addedJob.ID))

	// Phase 4: BullMQ observes "waiting" (no worker is running so the
	// state stays put for the assertion window).
	var afterRetry struct {
		State string `json:"state"`
	}
	runNodeInspector(t, prefix, queueName, &afterRetry, "getState", addedJob.ID)
	assert.Equal(t, "waiting", afterRetry.State, "BullMQ Job.getState must reflect mkq's RetryJob")
}

// assertionErr is a tiny error helper kept outside the test to avoid
// allocation churn in the failing-handler hot loop.
type assertionErr string

func (e assertionErr) Error() string { return string(e) }

// TestInterop_Mutation_DrainPending: mkq drains 3 wait jobs; BullMQ
// TS Queue.getJobCounts confirms wait went to 0 while delayed (10
// minutes out) survived.
func TestInterop_Mutation_DrainPending(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "mut"
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

	require.NoError(t, queue.DrainPending(ctx))

	var got struct {
		Counts struct {
			Wait    int64 `json:"wait"`
			Delayed int64 `json:"delayed"`
		} `json:"counts"`
	}
	runNodeInspector(t, prefix, queueName, &got, "counts", "wait", "delayed")
	assert.EqualValues(t, 0, got.Counts.Wait, "wait must be empty after Drain")
	assert.EqualValues(t, 2, got.Counts.Delayed, "delayed must survive Drain (default WithoutDrainDelayed)")
}
