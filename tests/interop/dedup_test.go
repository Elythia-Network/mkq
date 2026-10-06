//go:build interop

// Cross-language deduplication: a second Queue.add carrying the same
// dedup id within the TTL window must NOT create a new job. mkq and
// BullMQ TS share the dedup key shape (`{prefix}:{queue}:de:<id>`)
// and the lua's deduplicateJob include enforces the wire-level
// invariant — this test pins both directions stay honest.

package interop_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInterop_Dedup_MkqThenBullMQ: mkq Adds with a dedup id, then
// BullMQ TS Adds the same dedup id within ttl. BullMQ TS's
// deduplicateJob lua returns the existing job id rather than
// creating a new one — visible as the second add reporting the
// first add's job id.
func TestInterop_Dedup_MkqThenBullMQ(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "dd"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := queue.Add(ctx, interopPayload{Inbox: "first"},
		mkq.WithDeduplication("xlang", 10*time.Second),
	)
	require.NoError(t, err)

	// BullMQ TS Adds with the same dedup id; deduplicateJob lua
	// short-circuits and returns the existing job id.
	bullmqID := runNodeEnqueuerOpts(t, prefix, queueName, `{"inbox":"second"}`,
		nodeEnqueuerOpts{
			OptsJSON: `{"deduplication":{"id":"xlang","ttl":10000}}`,
		})
	assert.Equal(t, first.ID, bullmqID, "second BullMQ Add must surface the existing job id")
}

// TestInterop_Dedup_BullMQThenMkq: inverse direction — BullMQ TS
// Adds first; mkq Add with the same id observes the existing id and
// returns ErrDuplicateJob.
func TestInterop_Dedup_BullMQThenMkq(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "dd"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bullmqID := runNodeEnqueuerOpts(t, prefix, queueName, `{"inbox":"first"}`,
		nodeEnqueuerOpts{
			OptsJSON: `{"deduplication":{"id":"xlang2","ttl":10000}}`,
		})

	second, err := queue.Add(ctx, interopPayload{Inbox: "second"},
		mkq.WithDeduplication("xlang2", 10*time.Second),
	)
	// require.ErrorIs both stops on a non-dedup error AND validates
	// the sentinel. `require.Error + assert.ErrorIs + second.ID`
	// would panic with a nil deref if a real error (Redis hiccup,
	// Lua failure) slipped through unnoticed.
	require.ErrorIs(t, err, mkq.ErrDuplicateJob, "mkq must return ErrDuplicateJob when foreign-added job exists in dedup window")
	require.NotNil(t, second, "ErrDuplicateJob path must still return the existing Job[T] handle")
	assert.Equal(t, bullmqID, second.ID, "returned Job.ID must point at the existing BullMQ-side job")
}
