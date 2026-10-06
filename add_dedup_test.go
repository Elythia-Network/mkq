package mkq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestAddDedup_SuppressesWithinTTL pins the BullMQ deduplication
// contract: a second Add with the same dedup ID inside the TTL
// window does NOT create a new job. mkq surfaces the suppression
// via ErrDuplicateJob; the returned Job[T] points at the existing
// (first) job's ID so callers can still inspect it via Queue.Get.
func TestAddDedup_SuppressesWithinTTL(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := queue.Add(ctx, testPayload{Inbox: "first"},
		mkq.WithDeduplication("dedup-x", 10*time.Second),
	)
	require.NoError(t, err)

	second, err := queue.Add(ctx, testPayload{Inbox: "second"},
		mkq.WithDeduplication("dedup-x", 10*time.Second),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mkq.ErrDuplicateJob), "expected ErrDuplicateJob, got %v", err)
	assert.Equal(t, first.ID, second.ID, "second Add must surface the existing job ID")

	rdb := rawClient(t)
	count, err := rdb.LLen(ctx, prefix+":deliver:wait").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, count, "only one job should sit in wait")
}

// TestAddDedup_AllowsAfterTTL waits past the dedup TTL and verifies
// a second Add lands as a distinct job. This is the BullMQ analogue
// of asynq's `Unique(100ms)` allowing repeats once the window expires.
func TestAddDedup_AllowsAfterTTL(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := queue.Add(ctx, testPayload{},
		mkq.WithDeduplication("dedup-y", 100*time.Millisecond),
	)
	require.NoError(t, err)

	// PX expire is millisecond-resolution; 250ms cushion guards the
	// CI-flaky window between Redis tick rate and our test pace.
	time.Sleep(250 * time.Millisecond)

	second, err := queue.Add(ctx, testPayload{},
		mkq.WithDeduplication("dedup-y", 100*time.Millisecond),
	)
	require.NoError(t, err, "second Add after TTL must succeed")
	assert.NotEqual(t, first.ID, second.ID, "post-TTL Add must allocate a fresh job ID")
}

// TestAddDedup_DistinctIDsAreSeparate confirms that two different
// dedup IDs each get their own dedup window without cross-talk.
func TestAddDedup_DistinctIDsAreSeparate(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, err := queue.Add(ctx, testPayload{},
		mkq.WithDeduplication("a", 10*time.Second),
	)
	require.NoError(t, err)
	b, err := queue.Add(ctx, testPayload{},
		mkq.WithDeduplication("b", 10*time.Second),
	)
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, b.ID)
}

// TestAddDedup_RejectsEmptyID enforces the "id must be non-empty"
// guard documented on WithDeduplication. Passing "" is a programmer
// error (the option becomes a silent no-op without the guard).
func TestAddDedup_RejectsEmptyID(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{},
		mkq.WithDeduplication("", 10*time.Second),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be non-empty")
}

// TestAddDedup_UniqueAlias confirms the asynq-compat WithUnique alias
// behaves identically to WithDeduplication. mk-go's adapter shim
// reaches for the WithUnique name so a regression here would silently
// break asynq-style call sites in the port.
func TestAddDedup_UniqueAlias(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := queue.Add(ctx, testPayload{},
		mkq.WithUnique("alias-test", 10*time.Second),
	)
	require.NoError(t, err)

	_, err = queue.Add(ctx, testPayload{},
		mkq.WithUnique("alias-test", 10*time.Second),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mkq.ErrDuplicateJob))
	_ = first
}
