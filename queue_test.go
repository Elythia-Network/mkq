package mkq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// testRedisAddr is the integration-test Redis endpoint. The CI workflow
// brings up `redis:7-alpine` as a service container on this address;
// locally `redis-cli ping` against the default port works too.
const defaultTestRedisAddr = "127.0.0.1:6379"

func testRedisAddr() string {
	if v := os.Getenv("MKQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return defaultTestRedisAddr
}

type testPayload struct {
	Inbox string `json:"inbox"`
	Body  string `json:"body"`
}

// uniquePrefix derives a per-test BullMQ keyPrefix so concurrent runs
// don't collide on Redis state. Cleaned up via FLUSH-by-prefix on
// teardown.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
}

// stopWorker tears a worker down at test end.
//
// **Stop の戻り値を捨てない。** 失敗は「drain が終わらなかった」という情報で、
// goroutine と Redis 接続が残ったまま後続のテストに影響しうる。テスト自体を
// 落とすほどではないので log に出す — `defer w.Stop(ctx)` と書くと、その情報が
// どこにも出ずに消える。
func stopWorker(t *testing.T, w *mkq.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Stop(ctx); err != nil {
		t.Logf("worker did not stop cleanly: %v", err)
	}
}

func newClient(t *testing.T, prefix string) *mkq.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis: redis.UniversalOptions{
			Addrs: []string{testRedisAddr()},
		},
		KeyPrefix: prefix,
	})
	require.NoError(t, err, "mkq.NewClient")
	t.Cleanup(func() {
		flushPrefix(t, prefix)
		_ = c.Close()
	})
	return c
}

func flushPrefix(t *testing.T, prefix string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer rdb.Close()
	ctx := context.Background()
	pat := prefix + ":*"
	iter := rdb.Scan(ctx, 0, pat, 1000).Iterator()
	for iter.Next(ctx) {
		_ = rdb.Del(ctx, iter.Val()).Err()
	}
	require.NoError(t, iter.Err(), "scan/del prefix %s", prefix)
}

func rawClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestQueue_Add_StandardJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "https://example/inbox", Body: "hello"})
	require.NoError(t, err)
	require.NotEmpty(t, job.ID, "job id should be non-empty")

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// wait LIST should contain the new job id (FIFO push: LPUSH places
	// it at the head; workers RPOP).
	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, waitMembers, "wait should hold exactly the new job id")

	// HASH must carry BullMQ-shaped fields.
	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	require.Equal(t, "deliver", h["name"])
	var data testPayload
	require.NoError(t, json.Unmarshal([]byte(h["data"]), &data))
	require.Equal(t, "https://example/inbox", data.Inbox)
	require.Equal(t, "hello", data.Body)
	assert.Equal(t, "0", h["delay"])
	assert.Equal(t, "0", h["priority"])
	assert.NotEmpty(t, h["timestamp"])
	assert.NotEmpty(t, h["opts"])
}

func TestQueue_Add_DelayedJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	delay := 10 * time.Second
	before := time.Now().UnixMilli()
	job, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithDelay(delay))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// wait should be empty; delayed should hold the job.
	waitLen, err := rdb.LLen(ctx, base+"wait").Result()
	require.NoError(t, err)
	assert.Zero(t, waitLen, "wait must be empty for delayed job")

	delayedScores, err := rdb.ZRangeWithScores(ctx, base+"delayed", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, delayedScores, 1, "delayed should hold one entry")
	assert.Equal(t, job.ID, delayedScores[0].Member)

	// BullMQ encodes delayed score as `(timestamp << 12) | (counter & 0xFFF)`
	// so the high bits should be roughly delay+now.
	score := int64(delayedScores[0].Score)
	ts := score >> 12
	expectedMin := before + delay.Milliseconds()
	expectedMax := before + delay.Milliseconds() + 1000
	assert.GreaterOrEqual(t, ts, expectedMin, "delayed score timestamp should be >= now+delay")
	assert.LessOrEqual(t, ts, expectedMax, "delayed score timestamp should be near now+delay")
}

func TestQueue_Add_PrioritizedJob(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "p"}, mkq.WithPriority(7))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	// wait/delayed empty.
	waitLen, _ := rdb.LLen(ctx, base+"wait").Result()
	assert.Zero(t, waitLen)
	delayedLen, _ := rdb.ZCard(ctx, base+"delayed").Result()
	assert.Zero(t, delayedLen)

	// prioritized should hold the job; pc counter incremented.
	prioMembers, err := rdb.ZRangeWithScores(ctx, base+"prioritized", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, prioMembers, 1)
	assert.Equal(t, job.ID, prioMembers[0].Member)

	pc, err := rdb.Get(ctx, base+"pc").Result()
	require.NoError(t, err)
	pcVal, err := strconv.Atoi(pc)
	require.NoError(t, err)
	assert.Equal(t, 1, pcVal)

	// HASH priority field should match.
	h, err := rdb.HGetAll(ctx, base+job.ID).Result()
	require.NoError(t, err)
	assert.Equal(t, "7", h["priority"])
}

func TestQueue_Add_RejectsPriorityWithDelay(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{},
		mkq.WithPriority(3),
		mkq.WithDelay(10*time.Second),
	)
	assert.ErrorIs(t, err, mkq.ErrPriorityWithDelay)
}

func TestQueue_Add_SubMillisecondDelayRoutesToWait(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 500usはBullMQのms解像度では遅延ゼロ相当。delayedに迷い込むと
	// markerによるpromote待ちで永遠にdequeueされない (Devin指摘の bug)。
	job, err := queue.Add(ctx, testPayload{}, mkq.WithDelay(500*time.Microsecond))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":deliver:"

	delayedLen, err := rdb.ZCard(ctx, base+"delayed").Result()
	require.NoError(t, err)
	require.Zero(t, delayedLen, "sub-ms delay must not land on delayed ZSET")

	waitMembers, err := rdb.LRange(ctx, base+"wait", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, waitMembers, "sub-ms delay must land on wait LIST")
}

func TestQueue_Add_RespectsCustomJobID(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, testPayload{Inbox: "x"}, mkq.WithJobID("my-custom-id"))
	require.NoError(t, err)
	assert.Equal(t, "my-custom-id", job.ID)

	rdb := rawClient(t)
	exists, err := rdb.Exists(ctx, prefix+":deliver:my-custom-id").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, exists, "HASH should exist at the custom-id key")
}
