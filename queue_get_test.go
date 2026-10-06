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

// TestQueue_DefineWritesMetaVersion pins that Define stamps the BullMQ
// `meta.version` HASH field with the mkq version tag, and that the
// HSETNX semantics let a foreign value (e.g. a queue already
// initialised by BullMQ TS) survive untouched.
func TestQueue_DefineWritesMetaVersion(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	rdb := rawClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fresh Define stamps the field.
	_ = mkq.Define[testPayload](c, "deliver")
	got, err := rdb.HGet(ctx, prefix+":deliver:meta", "version").Result()
	require.NoError(t, err)
	assert.True(t, len(got) > len("mkq:") && got[:4] == "mkq:",
		"meta.version must start with `mkq:`, got %q", got)

	// Pre-populate a foreign value, re-Define, ensure it's NOT
	// overwritten (HSETNX semantics).
	require.NoError(t, rdb.HSet(ctx, prefix+":foreign:meta", "version", "bullmq:5.76.2").Err())
	_ = mkq.Define[testPayload](c, "foreign")
	got2, err := rdb.HGet(ctx, prefix+":foreign:meta", "version").Result()
	require.NoError(t, err)
	assert.Equal(t, "bullmq:5.76.2", got2,
		"Define must not overwrite an existing meta.version (HSETNX)")
}

// TestJob_RuntimeFieldsPopulatedAtDequeue pins that the new Job[T]
// runtime fields (AttemptsMade, AttemptsStarted, StalledCounter,
// ProcessedBy) are populated from the moveToActive HGETALL snapshot
// at the moment the handler runs.
func TestJob_RuntimeFieldsPopulatedAtDequeue(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Inbox: "x"},
		mkq.WithAttempts(3),
		mkq.WithBackoff(mkq.FixedBackoff(20*time.Millisecond)),
	)
	require.NoError(t, err)

	type capture struct {
		attemptsMade    int
		attemptsStarted int
		processedBy     string
	}
	captured := make(chan capture, 5)
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		captured <- capture{
			attemptsMade:    j.AttemptsMade,
			attemptsStarted: j.AttemptsStarted,
			processedBy:     j.ProcessedBy,
		}
		return nil, errors.New("force retry")
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	// Three attempts: atm=0,1,2 in order; ats increments each dequeue;
	// pb is non-empty (worker name) on every attempt.
	got := []capture{
		receiveOrFail(t, ctx, captured),
		receiveOrFail(t, ctx, captured),
		receiveOrFail(t, ctx, captured),
	}
	assert.Equal(t, 0, got[0].attemptsMade, "first attempt: atm must be 0")
	assert.Equal(t, 1, got[1].attemptsMade, "second attempt: atm must be 1")
	assert.Equal(t, 2, got[2].attemptsMade, "third attempt: atm must be 2")
	for i, c := range got {
		assert.GreaterOrEqual(t, c.attemptsStarted, i+1, "ats monotonic on attempt %d", i)
		assert.NotEmpty(t, c.processedBy, "pb must be set on attempt %d", i)
	}
}

// TestQueue_Get_CompletedJob covers the success snapshot path:
// ProcessedOn / FinishedOn / ReturnValue all populate after a
// handler completes.
func TestQueue_Get_CompletedJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{Inbox: "x"})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return map[string]any{"ok": true}, nil
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", added.ID).Result()
		return v > 0
	})

	job, state, err := queue.Get(ctx, added.ID)
	require.NoError(t, err)

	assert.Equal(t, added.ID, job.ID)
	assert.Equal(t, "x", job.Data.Inbox)
	assert.False(t, state.ProcessedOn.IsZero(), "ProcessedOn must be populated")
	assert.False(t, state.FinishedOn.IsZero(), "FinishedOn must be populated")
	assert.NotEmpty(t, state.ReturnValue)
	var ret map[string]any
	require.NoError(t, json.Unmarshal(state.ReturnValue, &ret))
	assert.Equal(t, true, ret["ok"])
	assert.Empty(t, state.FailedReason)
	assert.Empty(t, state.Stacktrace)
}

// TestQueue_Get_FailedJob covers the failure snapshot path:
// FailedReason and Stacktrace populate.
func TestQueue_Get_FailedJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[testPayload]) (any, error) {
		return nil, errors.New("nope")
	}, mkq.WithIdlePollInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", added.ID).Result()
		return v > 0
	})

	_, state, err := queue.Get(ctx, added.ID)
	require.NoError(t, err)
	assert.Equal(t, "nope", state.FailedReason)
	require.NotEmpty(t, state.Stacktrace)
	assert.Equal(t, "nope", state.Stacktrace[0])
}

// TestQueue_Get_NotFound pins the ErrJobNotFound sentinel.
func TestQueue_Get_NotFound(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := queue.Get(ctx, "does-not-exist")
	assert.ErrorIs(t, err, mkq.ErrJobNotFound)
}
