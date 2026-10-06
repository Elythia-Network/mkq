package mkq_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestSchedulePattern_ImmediatelyFires validates that
// WithScheduleImmediately makes the first iteration fire at upsert
// time, without waiting for the next pattern-match. This decouples
// the test from the cron grain (`* * * * *` would otherwise force a
// wait of up to 60s for the first fire).
func TestSchedulePattern_ImmediatelyFires(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertSchedulePattern(ctx,
		"now", "* * * * *", testPayload{Inbox: "tick"},
		mkq.WithScheduleImmediately(),
	))

	var seen atomic.Int64
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[testPayload]) (any, error) {
		assert.True(t, strings.HasPrefix(j.ID, "repeat:now:"), "unexpected job id %q", j.ID)
		seen.Add(1)
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	waitFor(t, ctx, 50*time.Millisecond, func() bool { return seen.Load() >= 1 })

	rdb := rawClient(t)
	got, err := rdb.HGetAll(ctx, prefix+":tick:repeat:now").Result()
	require.NoError(t, err)
	assert.Equal(t, "* * * * *", got["pattern"], "schedule HASH must record pattern")
	assert.NotEmpty(t, got["ic"], "ic counter must be set after first fire")
}

// TestSchedulePattern_HASHCarriesTimezone confirms that the tz
// option lands in the schedule HASH so foreign clients (BullMQ TS)
// can pick it up unchanged.
func TestSchedulePattern_HASHCarriesTimezone(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertSchedulePattern(ctx,
		"tz", "0 9 * * *", testPayload{},
		mkq.WithScheduleTimezone("Asia/Tokyo"),
	))

	rdb := rawClient(t)
	got, err := rdb.HGetAll(ctx, prefix+":tick:repeat:tz").Result()
	require.NoError(t, err)
	assert.Equal(t, "0 9 * * *", got["pattern"])
	assert.Equal(t, "Asia/Tokyo", got["tz"])
}

// TestSchedulePattern_RejectsExoticSyntax pins the documented v1
// subset by checking that syntax mkq does not promise to handle is
// rejected at upsert time rather than silently producing
// BullMQ-incompatible iterations.
func TestSchedulePattern_RejectsExoticSyntax(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cases := map[string]string{
		"6-field seconds":                  "0 * * * * *",
		"@every (use UpsertScheduleEvery)": "@every 5m",
		"empty pattern":                    "",
		"random garbage":                   "not a cron",
	}
	for name, pat := range cases {
		t.Run(name, func(t *testing.T) {
			err := queue.UpsertSchedulePattern(ctx, "x", pat, testPayload{})
			assert.Error(t, err, "pattern %q should be rejected", pat)
		})
	}
}

// TestSchedulePattern_RejectsBogusTimezone catches a typo / unknown
// IANA name before it gets persisted to the schedule HASH.
func TestSchedulePattern_RejectsBogusTimezone(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := queue.UpsertSchedulePattern(ctx, "x", "0 9 * * *", testPayload{},
		mkq.WithScheduleTimezone("Mars/Olympus"),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timezone")
}

// TestSchedulePattern_RejectsImmediatelyWithStartDate mirrors BullMQ
// TS's mutual-exclusion validation in upsertJobScheduler.
func TestSchedulePattern_RejectsImmediatelyWithStartDate(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := queue.UpsertSchedulePattern(ctx, "x", "* * * * *", testPayload{},
		mkq.WithScheduleImmediately(),
		mkq.WithScheduleStartDate(time.Now().Add(time.Hour)),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

// TestScheduleEvery_RejectsPatternOnlyOptions keeps every-mode and
// pattern-mode option surfaces from leaking into each other. Passing
// WithScheduleTimezone or WithScheduleImmediately to UpsertScheduleEvery
// is a programmer error — those options have no meaning for fixed
// intervals — so the upsert call fails fast rather than silently
// dropping the option.
func TestScheduleEvery_RejectsPatternOnlyOptions(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "tick")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("timezone", func(t *testing.T) {
		err := queue.UpsertScheduleEvery(ctx, "x", time.Second, testPayload{},
			mkq.WithScheduleTimezone("UTC"),
		)
		require.Error(t, err)
	})
	t.Run("immediately", func(t *testing.T) {
		err := queue.UpsertScheduleEvery(ctx, "x", time.Second, testPayload{},
			mkq.WithScheduleImmediately(),
		)
		require.Error(t, err)
	})
}
