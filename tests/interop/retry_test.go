//go:build interop

// TestInterop_RetryBackoff: mkq Add with WithAttempts + WithBackoff;
// BullMQ TS Worker fails the handler each time. The vendored
// retryJob-11.lua + moveToDelayed-11.lua wire path drives the job
// through 3 attempts before landing in failed, and BullMQ TS
// observes the final HASH state (atm = attempts, processedOn /
// finishedOn populated, failedReason set).
//
// This pins that mkq's worker contract for handing failed-but-
// retryable jobs back to the lua matches what BullMQ TS produces
// (= a foreign worker can take over an mkq-added job mid-retry-cycle).

package interop_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

func TestInterop_RetryBackoff(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "retry"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const attempts = 3
	addedJob, err := queue.Add(ctx, interopPayload{Inbox: "retry"},
		mkq.WithAttempts(attempts),
		mkq.WithBackoff(mkq.FixedBackoff(50*time.Millisecond)),
	)
	require.NoError(t, err)

	// Failing BullMQ TS worker drives the job through all attempts.
	startNodeWorkerOpts(t, prefix, queueName, nodeWorkerOpts{Fail: true})

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", addedJob.ID).Result()
		return v > 0
	})

	atm, err := rdb.HGet(ctx, base+addedJob.ID, "atm").Result()
	require.NoError(t, err)
	assert.Equal(t, "3", atm, "BullMQ-driven retries must converge on attemptsMade=attempts")

	failedReason, err := rdb.HGet(ctx, base+addedJob.ID, "failedReason").Result()
	require.NoError(t, err)
	assert.Contains(t, failedReason, "intentional failure",
		"failedReason must surface the BullMQ-side handler error verbatim")
}
