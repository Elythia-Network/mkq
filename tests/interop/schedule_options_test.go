//go:build interop

// Schedule-options coverage for the cross-language scheduler interop.
// PR #33/#35 already pin the every / pattern + WithScheduleImmediately
// paths; this file fills in WithScheduleLimit / WithScheduleEndDate /
// WithScheduleTimezone — each verified by reading the schedule HASH
// from BullMQ TS's perspective (Queue.getJobScheduler isn't exposed
// in mkq's npm pin, so we assert via raw HGETALL via a small helper).
//
// Direction is mkq→BullMQ; the schedule HASH must match BullMQ's
// expected field shape so a foreign worker continues iteration.

package interop_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

func TestInterop_ScheduleOptions_Limit(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "tick"
	const scheduleID = "limit"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx,
		scheduleID, time.Hour, interopPayload{Inbox: "x"},
		mkq.WithScheduleLimit(5),
	))

	rdb := rawClient(t)
	got, err := rdb.HGetAll(ctx, prefix+":"+queueName+":repeat:"+scheduleID).Result()
	require.NoError(t, err)
	assert.Equal(t, "5", got["limit"], "BullMQ-readable HASH must record `limit` field")
}

func TestInterop_ScheduleOptions_EndDate(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "tick"
	const scheduleID = "endd"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	end := time.Now().Add(2 * time.Hour)
	require.NoError(t, queue.UpsertScheduleEvery(ctx,
		scheduleID, time.Minute, interopPayload{},
		mkq.WithScheduleEndDate(end),
	))

	rdb := rawClient(t)
	got, err := rdb.HGetAll(ctx, prefix+":"+queueName+":repeat:"+scheduleID).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, got["endDate"], "BullMQ-readable HASH must record `endDate`")
}

func TestInterop_ScheduleOptions_Timezone(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "tick"
	const scheduleID = "tz"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertSchedulePattern(ctx,
		scheduleID, "0 9 * * *", interopPayload{},
		mkq.WithScheduleTimezone("Asia/Tokyo"),
	))

	rdb := rawClient(t)
	got, err := rdb.HGetAll(ctx, prefix+":"+queueName+":repeat:"+scheduleID).Result()
	require.NoError(t, err)
	assert.Equal(t, "Asia/Tokyo", got["tz"], "BullMQ-readable HASH must record `tz`")
	assert.Equal(t, "0 9 * * *", got["pattern"])
}
