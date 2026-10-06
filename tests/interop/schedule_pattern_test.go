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

// TestInterop_MkqPatternBullMQProcess: mkq UpsertSchedulePattern
// writes the schedule HASH (pattern, tz fields), and a BullMQ TS
// Worker subprocess processes the immediate first iteration. The
// asserted invariant is that BullMQ TS reads mkq's HASH layout
// without errors and that the iteration job ID matches the
// repeat:<id>:<millis> convention both sides agree on.
//
// We deliberately use WithScheduleImmediately to avoid waiting for
// the next minute boundary; second-iteration timing is a function
// of cron grain (1 minute), not interop correctness.
func TestInterop_MkqPatternBullMQProcess(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "tick"
	const scheduleID = "interop-cron"

	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	startNodeWorker(t, prefix, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertSchedulePattern(ctx,
		scheduleID, "* * * * *",
		interopPayload{Inbox: "https://x", Body: "tick"},
		mkq.WithScheduleImmediately(),
	))

	rdb := rawClient(t)
	scheduleKey := prefix + ":" + queueName + ":repeat:" + scheduleID
	completed := prefix + ":" + queueName + ":completed"

	// 即時 fire の後、BullMQ TS は次 iteration を minute boundary に
	// スケジュールする。最初の 1 件が completed に入るところまで待てば、
	// HASH layout 互換性は十分検証できる。
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		n, _ := rdb.ZCard(ctx, completed).Result()
		return n >= 1
	})

	ids, err := rdb.ZRange(ctx, completed, 0, -1).Result()
	require.NoError(t, err)
	for _, id := range ids {
		assert.True(t, strings.HasPrefix(id, "repeat:"+scheduleID+":"),
			"unexpected jobId %q in completed set", id)
	}

	got, err := rdb.HGetAll(ctx, scheduleKey).Result()
	require.NoError(t, err)
	assert.Equal(t, queueName, got["name"], "schedule HASH name field")
	assert.Equal(t, "* * * * *", got["pattern"], "pattern field preserved")
	assert.NotEmpty(t, got["ic"], "ic counter must be set")
}
