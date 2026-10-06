package mkq_test

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestInspector_Queues_TracksDefine pins the SADD-on-Define contract:
// every Queue[T] handle stitched into a Client appears in
// Client.Queues, idempotent across repeat Define calls.
func TestInspector_Queues_TracksDefine(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	mkq.Define[testPayload](c, "alpha")
	mkq.Define[testPayload](c, "beta")
	mkq.Define[testPayload](c, "gamma")
	mkq.Define[testPayload](c, "alpha") // duplicate Define must not duplicate registry entry

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.Queues(ctx)
	require.NoError(t, err)
	sort.Strings(got)
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, got)
}

// TestInspector_Counts_PerState exercises the multi-bucket count path.
// We seed three jobs into wait, two into delayed (via WithDelay), and
// run one to completion — the resulting Counts must reflect each
// bucket independently.
func TestInspector_Counts_PerState(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 3 wait jobs.
	for i := range 3 {
		_, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
	}
	// 2 delayed jobs (10 minutes out so they don't promote during the test).
	for range 2 {
		_, err := queue.Add(ctx, testPayload{Inbox: "later"}, mkq.WithDelay(10*time.Minute))
		require.NoError(t, err)
	}
	// 1 completed job: write directly into the completed ZSET. We
	// deliberately bypass the worker — running Process in this test
	// would drain the wait LIST as a side effect, making the
	// per-bucket assertions racy. Inspector logic only reads ZCARD,
	// so a synthetic ZADD is wire-equivalent for the count check.
	rdb := rawClient(t)
	base := prefix + ":deliver:"
	require.NoError(t, rdb.ZAdd(ctx, base+"completed", redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: "synthetic-completed",
	}).Err())

	counts, err := queue.Counts(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, counts.Wait, "3 plain Add()s should land in wait")
	assert.EqualValues(t, 2, counts.Delayed, "2 WithDelay Add()s should land in delayed")
	assert.EqualValues(t, 1, counts.Completed, "1 successfully processed job should land in completed")
	assert.EqualValues(t, 0, counts.Active, "no in-flight handlers after Stop")
	assert.EqualValues(t, 0, counts.Failed)

	// Subset call returns only the requested bucket fields.
	subset, err := queue.Counts(ctx, mkq.JobBucketWait, mkq.JobBucketDelayed)
	require.NoError(t, err)
	assert.EqualValues(t, 3, subset.Wait)
	assert.EqualValues(t, 2, subset.Delayed)
	assert.EqualValues(t, 0, subset.Completed, "Completed should be zero — not requested")
}

// TestInspector_ListJobs_Pagination exercises the (start, end) range
// semantics against a known wait-queue seed. The lua's LIST handling
// for the wait bucket reads in BullMQ insertion order.
func TestInspector_ListJobs_Pagination(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 5 jobs: payload Inbox carries the index so we can verify order.
	added := make([]string, 5)
	for i := range 5 {
		j, err := queue.Add(ctx, testPayload{Inbox: strconv.Itoa(i)})
		require.NoError(t, err)
		added[i] = j.ID
	}

	// Two pages collectively cover the 5 added IDs. Within-page
	// ordering is bucket-specific (BullMQ's wait LIST is LPUSH-backed
	// so newest-first is the lua-native shape) — exposing the exact
	// per-page ordering as a contract would over-specify and trip
	// future BullMQ tweaks. The contract callers care about is
	// "every job appears exactly once across the pages."
	page1, err := queue.ListJobs(ctx, mkq.JobBucketWait, 0, 1, false)
	require.NoError(t, err)
	require.Len(t, page1, 2)

	page2, err := queue.ListJobs(ctx, mkq.JobBucketWait, 2, -1, false)
	require.NoError(t, err)
	require.Len(t, page2, 3)

	gotIDs := make([]string, 0, 5)
	for _, j := range page1 {
		gotIDs = append(gotIDs, j.Job.ID)
	}
	for _, j := range page2 {
		gotIDs = append(gotIDs, j.Job.ID)
	}
	sort.Strings(gotIDs)
	wantIDs := append([]string(nil), added...)
	sort.Strings(wantIDs)
	assert.Equal(t, wantIDs, gotIDs, "every added job must appear exactly once across the pages")
}

// TestInspector_ListJobs_TypedPayload pins the buildJob[T] round-trip:
// the ListedJob.Job.Data field decodes into the user's T (testPayload),
// and the JobState companion is populated even for in-flight jobs
// (timestamp fields stay zero, which is the documented behaviour).
func TestInspector_ListJobs_TypedPayload(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{Inbox: "https://example.org", Body: "hello"})
	require.NoError(t, err)

	listed, err := queue.ListJobs(ctx, mkq.JobBucketWait, 0, -1, true)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	got := listed[0]
	assert.Equal(t, "https://example.org", got.Job.Data.Inbox)
	assert.Equal(t, "hello", got.Job.Data.Body)
	require.NotNil(t, got.State)
	assert.True(t, got.State.ProcessedOn.IsZero(), "untouched job should have zero ProcessedOn")
	assert.True(t, got.State.FinishedOn.IsZero(), "untouched job should have zero FinishedOn")
}

// TestInspector_ListJobs_EmptyState returns an empty slice (not nil)
// for empty buckets so callers can range over the result without a
// nil check.
func TestInspector_ListJobs_EmptyState(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "empty")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, b := range []mkq.JobBucket{
		mkq.JobBucketWait, mkq.JobBucketActive, mkq.JobBucketDelayed,
		mkq.JobBucketCompleted, mkq.JobBucketFailed,
	} {
		t.Run(string(b), func(t *testing.T) {
			got, err := queue.ListJobs(ctx, b, 0, -1, true)
			require.NoError(t, err)
			assert.NotNil(t, got, "empty bucket must return non-nil slice for safe range")
			assert.Len(t, got, 0)
		})
	}
}
