//go:build interop

package interop_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInterop_MkqScheduleBullMQProcess: mkq UpsertScheduleEvery sets
// the schedule template (HASH at `repeat:<id>`) and the first delayed
// instance. A BullMQ TS Worker subprocess then picks up each iteration
// and is responsible for calling updateJobScheduler-12 for the next.
//
// This test asserts that mkq's schedule HASH layout (field names,
// every / data / opts encoding, ic counter) is interpretable by
// stock BullMQ TS — if it weren't, the worker would fail to advance
// `ic` past the first iteration.
func TestInterop_MkqScheduleBullMQProcess(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "tick"
	const scheduleID = "interop-tick"

	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	startNodeWorker(t, prefix, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx,
		scheduleID, 200*time.Millisecond,
		interopPayload{Inbox: "https://x", Body: "tick"},
	))

	rdb := rawClient(t)
	scheduleKey := prefix + ":" + queueName + ":repeat:" + scheduleID
	completed := prefix + ":" + queueName + ":completed"

	// BullMQ TS worker が複数 iteration を処理し、
	// 各 iteration の後に updateJobScheduler-12 で ic を bump。
	// 3 回以上発火するのを待つ。
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, completed).Result()
		return n >= 3
	})

	// 全ての完了 job が repeat:<id>:* prefix を持つことを確認。
	ids, err := rdb.ZRange(ctx, completed, 0, -1).Result()
	require.NoError(t, err)
	for _, id := range ids {
		assert.True(t, strings.HasPrefix(id, "repeat:"+scheduleID+":"),
			"unexpected jobId %q in completed set", id)
	}

	// schedule HASH の name / every が mkq の登録通りであり、
	// BullMQ TS worker が ic を bump し続けていることを確認。
	got, err := rdb.HGetAll(ctx, scheduleKey).Result()
	require.NoError(t, err)
	assert.Equal(t, queueName, got["name"], "schedule HASH name field")
	assert.Equal(t, "200", got["every"], "every field preserved across iterations")
	assert.NotEmpty(t, got["ic"], "ic counter must be set")
}
