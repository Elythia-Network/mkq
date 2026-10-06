//go:build interop

// Package interop_test exercises mkq <-> BullMQ TS bidirectional
// scenarios against a real Redis. These are smoke tests, not a
// substitute for unit/integration tests; run them via:
//
//	cd tests/interop/node && npm install   # one-time
//	go test -tags=interop ./tests/interop/...
//
// Each test orchestrates a small node subprocess that imports the
// `bullmq` npm package pinned in tests/interop/node/package.json.
// The pin matches the third_party/bullmq submodule SHA so the
// vendored Lua and the BullMQ-TS implementation under test came
// from the same upstream commit.
package interop_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

func redisAddr() string {
	if v := os.Getenv("MKQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}

func uniquePrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("mkqinterop-%s-%d", t.Name(), time.Now().UnixNano())
}

func nodeDir(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "node")
	if _, err := os.Stat(filepath.Join(dir, "node_modules")); err != nil {
		t.Fatalf("npm deps missing under %s: run 'cd tests/interop/node && npm install' first", dir)
	}
	return dir
}

func newClient(t *testing.T, prefix string) *mkq.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{redisAddr()}},
		KeyPrefix: prefix,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		flushPrefix(t, prefix)
		_ = c.Close()
	})
	return c
}

func flushPrefix(t *testing.T, prefix string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	defer rdb.Close()
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, prefix+":*", 1000).Iterator()
	for iter.Next(ctx) {
		_ = rdb.Del(ctx, iter.Val()).Err()
	}
}

func rawClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func waitFor(t *testing.T, ctx context.Context, step time.Duration, cond func() bool) {
	t.Helper()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waitFor: timed out: %v", ctx.Err())
		case <-time.After(step):
		}
	}
}

// startNodeWorker spawns the BullMQ TS worker as a subprocess with
// default options. Returns once worker.js prints {"event":"ready"}
// so callers can race the next job submission without a fixed sleep.
// Cleanup is registered on t.
//
// Thin wrapper over startNodeWorkerOpts so the ready-channel /
// stdout-drain / cleanup machinery lives in exactly one place.
func startNodeWorker(t *testing.T, prefix, queueName string) {
	t.Helper()
	startNodeWorkerOpts(t, prefix, queueName, nodeWorkerOpts{})
}

// runNodeEnqueuer runs enqueuer.js once with default options and
// returns the Job ID it reported. Synchronous: returns after the
// subprocess exits.
//
// Thin wrapper over runNodeEnqueuerOpts so the exec / env / JSON-parse
// machinery lives in exactly one place; PR #5–#46's existing tests
// keep using this short signature.
func runNodeEnqueuer(t *testing.T, prefix, queueName, payloadJSON string) string {
	t.Helper()
	return runNodeEnqueuerOpts(t, prefix, queueName, payloadJSON, nodeEnqueuerOpts{})
}

type interopPayload struct {
	Inbox string `json:"inbox"`
	Body  string `json:"body"`
}

// TestInterop_MkqAddBullMQProcess: mkq Queue.Add -> BullMQ TS Worker
// processes -> assert wire state from mkq side.
func TestInterop_MkqAddBullMQProcess(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "deliver"

	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	startNodeWorker(t, prefix, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	job, err := queue.Add(ctx, interopPayload{Inbox: "https://x", Body: "from-mkq"})
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", job.ID).Result()
		return v > 0
	})

	rv, err := rdb.HGet(ctx, base+job.ID, "returnvalue").Result()
	require.NoError(t, err)
	var ret struct {
		By    string         `json:"by"`
		JobID string         `json:"jobId"`
		Echo  interopPayload `json:"echo"`
	}
	require.NoError(t, json.Unmarshal([]byte(rv), &ret))
	assert.Equal(t, "bullmq-ts", ret.By, "BullMQ TS worker must have processed the job")
	assert.Equal(t, job.ID, ret.JobID)
	assert.Equal(t, "from-mkq", ret.Echo.Body)
}

// TestInterop_BullMQAddMkqProcess: BullMQ TS Queue.add -> mkq Process
// handler -> assert.
func TestInterop_BullMQAddMkqProcess(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "deliver"

	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	type handlerCapture struct {
		jobID string
		data  interopPayload
	}
	var captured atomic.Value
	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[interopPayload]) (any, error) {
		captured.Store(handlerCapture{jobID: j.ID, data: j.Data})
		return map[string]any{"by": "mkq", "jobId": j.ID}, nil
	},
		mkq.WithIdlePollInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	jobID := runNodeEnqueuer(t, prefix, queueName,
		`{"inbox":"https://y","body":"from-bullmq"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", jobID).Result()
		return v > 0
	})

	got, _ := captured.Load().(handlerCapture)
	assert.Equal(t, jobID, got.jobID, "mkq handler must see the BullMQ-added job")
	assert.Equal(t, "from-bullmq", got.data.Body)

	rv, err := rdb.HGet(ctx, base+jobID, "returnvalue").Result()
	require.NoError(t, err)
	assert.Contains(t, rv, `"by":"mkq"`, "returnvalue must reflect mkq handler")
}
