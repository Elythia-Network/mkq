package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// TestDiscoverQueues_FindsQueuesMkqNeverDefined is the point of the
// API: Queues only knows what this client touched, so a queue created
// by a BullMQ worker in another language — or another process — is
// invisible to it.
func TestDiscoverQueues_FindsQueuesMkqNeverDefined(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// mkq 経由で 1 本。
	mine := mkq.Define[testPayload](c, "mine")
	_, err := mine.Add(ctx, testPayload{})
	require.NoError(t, err)

	// 他言語のワーカーが作ったキューを模して、meta キーだけ直接置く。
	rdb := rawClient(t)
	require.NoError(t, rdb.HSet(ctx, prefix+":foreign:meta", "opts.maxLenEvents", "10000").Err())

	registered, err := c.Queues(ctx)
	require.NoError(t, err)
	assert.NotContains(t, registered, "foreign", "Queues cannot see a queue it never Define'd")

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, found, "mine")
	assert.Contains(t, found, "foreign")
}

// Define writes the queue's meta key, so a queue with no jobs at all is
// still discoverable — which is why there is no separate "also fold in
// the registry" mode.
func TestDiscoverQueues_FindsADefinedQueueWithNoJobs(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mkq.Define[testPayload](c, "never-used")

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, found, "never-used")

}

// Queue names may contain the key separator; peeling a fixed prefix and
// a fixed suffix gets them back without ambiguity.
func TestDiscoverQueues_HandlesColonsInQueueNames(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := mkq.Define[testPayload](c, "tenant:a:deliver")
	_, err := q.Add(ctx, testPayload{})
	require.NoError(t, err)

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, found, "tenant:a:deliver")
}

// A job id is an arbitrary string, so one can produce a key that ends
// in ":meta" and masquerade as a queue. Job hashes carry a data field
// and queue metadata does not, which is what tells them apart.
func TestDiscoverQueues_IgnoresJobsThatLookLikeQueues(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := mkq.Define[testPayload](c, "deliver")
	_, err := q.Add(ctx, testPayload{}, mkq.WithJobID("evil:meta"))
	require.NoError(t, err)

	rdb := rawClient(t)
	exists, err := rdb.Exists(ctx, prefix+":deliver:evil:meta").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, exists, "the job hash that shadows a meta key must exist for this test to mean anything")

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Contains(t, found, "deliver")
	assert.NotContains(t, found, "deliver:evil", "a job hash must not be reported as a queue")
}

func TestDiscoverQueues_EmptyKeyspace(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Empty(t, found)
}

// Results come back sorted. (They are also deduplicated, because SCAN
// guarantees only at-least-once delivery of a key — that half cannot be
// forced from a test, so it is not claimed here.)
func TestDiscoverQueues_SortedResult(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, name := range []string{"zulu", "alpha", "mike"} {
		q := mkq.Define[testPayload](c, name)
		_, err := q.Add(ctx, testPayload{})
		require.NoError(t, err)
	}

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "mike", "zulu"}, found)
}

// A COUNT small enough to force several SCAN round-trips must still
// return everything.
func TestDiscoverQueues_PaginatesAcrossScanCursors(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var want []string
	for i := 0; i < 25; i++ {
		name := "q" + string(rune('a'+i%25))
		q := mkq.Define[testPayload](c, name)
		_, err := q.Add(ctx, testPayload{})
		require.NoError(t, err)
		want = append(want, name)
	}

	found, err := c.DiscoverQueues(ctx, mkq.WithDiscoverCount(1))
	require.NoError(t, err)
	for _, name := range want {
		assert.Contains(t, found, name)
	}
}

// mkq's own registry lives at "{prefix}::queues" — the empty
// queue-name slot, chosen because BullMQ requires a non-empty name and
// so can never produce it. A meta key in that same slot must not come
// back as a queue with no name.
func TestDiscoverQueues_RejectsTheEmptyQueueNameSlot(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := mkq.Define[testPayload](c, "deliver")
	_, err := q.Add(ctx, testPayload{})
	require.NoError(t, err)

	rdb := rawClient(t)
	require.NoError(t, rdb.HSet(ctx, prefix+"::meta", "version", "x").Err())

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.NotContains(t, found, "", "the empty queue-name slot is not a queue")
	assert.Equal(t, []string{"deliver"}, found)
}

// A deduplication key is a STRING living at "{prefix}:{queue}:de:<id>",
// and the id is whatever the caller passed. One ending in ":meta" lands
// in the scan results, and a hash command against it would fail the
// whole pipeline — taking every queue with it.
func TestDiscoverQueues_SurvivesANonHashKeyInTheWay(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := mkq.Define[testPayload](c, "deliver")
	_, err := q.Add(ctx, testPayload{}, mkq.WithDeduplication("tenant:meta", time.Minute))
	require.NoError(t, err)

	rdb := rawClient(t)
	typ, err := rdb.Type(ctx, prefix+":deliver:de:tenant:meta").Result()
	require.NoError(t, err)
	require.Equal(t, "string", typ, "this test only means something while the dedup key is a STRING")

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err, "a non-hash key in the namespace must not fail the whole call")
	assert.Equal(t, []string{"deliver"}, found)
}

// A job-scheduler hash sits at "{prefix}:{queue}:repeat:<id>" and, with
// no template data, carries none of the fields a job hash does. Only a
// positive test for the fields queue metadata actually has keeps it out.
func TestDiscoverQueues_IgnoresASchedulerHash(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := mkq.Define[testPayload](c, "deliver")
	_, err := q.Add(ctx, testPayload{})
	require.NoError(t, err)

	rdb := rawClient(t)
	require.NoError(t, rdb.HSet(ctx, prefix+":deliver:repeat:nightly:meta", "name", "nightly").Err())

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"deliver"}, found)
}

// The key prefix is configuration, so it can contain glob syntax. Left
// unescaped it turns the scan into a pattern that matches nothing, and
// the caller gets an empty list with no error — a silent miss.
func TestDiscoverQueues_PrefixWithGlobMetacharacters(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t) + "[prod]"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{testRedisAddr()}},
		KeyPrefix: prefix,
	})
	require.NoError(t, err)
	t.Cleanup(func() { flushPrefix(t, prefix); _ = c.Close() })

	q := mkq.Define[testPayload](c, "deliver")
	_, err = q.Add(ctx, testPayload{})
	require.NoError(t, err)

	found, err := c.DiscoverQueues(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"deliver"}, found)
}
