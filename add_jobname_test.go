package mkq_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestAddJobName_SetsHashField confirms the wire-level effect:
// the BullMQ HASH `name` field reflects what WithJobName passed,
// not the queue name. This is the field bull-board renders as the
// job's display label and the field mk-go's adapter dispatches on.
func TestAddJobName_SetsHashField(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{}, mkq.WithJobName("ap:deliver"))
	require.NoError(t, err)
	assert.Equal(t, "ap:deliver", job.Name, "Job.Name reflects the override")

	rdb := rawClient(t)
	got, err := rdb.HGet(ctx, prefix+":deliver:"+job.ID, "name").Result()
	require.NoError(t, err)
	assert.Equal(t, "ap:deliver", got, "BullMQ HASH `name` field must carry the override")
}

// TestAddJobName_DefaultIsQueueName pins the regression contract:
// without WithJobName, Job.Name remains the queue name (mkq's
// pre-#40 behaviour). Skipping this would silently leak the new
// option's behaviour into the default path.
func TestAddJobName_DefaultIsQueueName(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)
	assert.Equal(t, "deliver", job.Name)

	rdb := rawClient(t)
	got, err := rdb.HGet(ctx, prefix+":deliver:"+job.ID, "name").Result()
	require.NoError(t, err)
	assert.Equal(t, "deliver", got)
}

// TestAddJobName_HandlerSeesIt confirms the handler-side round trip:
// buildJob[T] reads the `name` HASH field into Job.Name so the
// adapter's per-task-type dispatch can read it without a separate
// Redis hop. This is the contract mk-go's MkqDriver depends on for
// asynq-style mux registration.
func TestAddJobName_HandlerSeesIt(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "maintenance")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithJobName("system:retention"))
	require.NoError(t, err)

	var seen atomic.Value
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		seen.Store(j.Name)
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":maintenance:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", added.ID).Result()
		return v > 0
	})

	got, _ := seen.Load().(string)
	assert.Equal(t, "system:retention", got, "handler must see the per-Add name override")
}

// TestAddJobName_RejectsEmpty enforces the "name must be non-empty"
// guard. Passing "" would silently disable the override and fall
// back to the queue name — surfacing the misuse as an error
// prevents that surprise.
func TestAddJobName_RejectsEmpty(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{}, mkq.WithJobName(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be non-empty")
}
