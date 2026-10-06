//go:build interop

// Cross-language stalled recovery: a BullMQ TS Worker takes a job
// (short lockDuration + long handler hold), gets killed mid-flight,
// and an mkq Worker's stalled scanner reclaims the orphaned job.
// Pins the lock-key wire format compatibility — same `:lock` STRING
// shape, same stalled SET membership, same maxStalledCount handling.

package interop_test

import (
	"context"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

func TestInterop_StalledRecovery(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "stl"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	addedJob, err := queue.Add(ctx, interopPayload{Inbox: "stalled"})
	require.NoError(t, err)

	// BullMQ TS Worker with short lockDuration + 30s handler hold —
	// designed to be killed mid-handler so the lock doesn't get
	// extended.
	dir := nodeDir(t)
	bullCmd := exec.Command("node", "worker.js")
	bullCmd.Dir = dir
	bullCmd.Env = append(os.Environ(),
		"INTEROP_REDIS="+redisAddr(),
		"INTEROP_PREFIX="+prefix,
		"INTEROP_QUEUE="+queueName,
		"INTEROP_LOCK_DURATION_MS=1500", // short, so lock expires after kill
		"INTEROP_HOLD_MS=30000",         // handler holds 30s = far longer than test budget
	)
	startWorkerProcess(t, bullCmd)

	// Wait for the job to land in active under BullMQ's lock.
	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		exists, _ := rdb.Exists(ctx, base+addedJob.ID+":lock").Result()
		return exists == 1
	})

	// Hard-kill BullMQ TS so its heartbeat stops extending the lock.
	// startWorkerProcess registered a t.Cleanup that will SIGINT and
	// re-Wait this same process; that's intentional and safe — Wait
	// after a successful Wait returns "exec: Wait was already called",
	// the cleanup discards the error via `_`. If a future change adds
	// strict error checking to the cleanup path, this kill-then-wait
	// dance must be relocated into its own Cleanup that fires before
	// the helper's.
	require.NoError(t, bullCmd.Process.Kill())
	_ = bullCmd.Wait()

	// Wait > lockDuration + a generous margin so the lock is reliably
	// gone before mkq's stalled scanner starts looking. With
	// lockDuration=1500ms the worst-case expiry is ~1.5s after kill.
	time.Sleep(3 * time.Second)
	// Sanity: lock must be gone by now.
	lockExists, err := rdb.Exists(ctx, base+addedJob.ID+":lock").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, lockExists, "BullMQ lock did not expire within the test budget")

	// BullMQ's worker writes the stalled-check key with its own
	// (much longer) interval — TTL of ~30s by default. Both libraries
	// share this key, so mkq's scanner short-circuits if BullMQ's
	// stamp is still alive. Clear it so mkq's scanner can run.
	_ = rdb.Del(ctx, base+"stalled-check").Err()

	// Diagnostic: confirm the job is still in active (BullMQ left it
	// behind when killed) so mkq's stalled scanner has something to find.
	activeIDs, _ := rdb.LRange(ctx, base+"active", 0, -1).Result()
	t.Logf("active list after kill+sleep: %v", activeIDs)
	stalledIDs, _ := rdb.SMembers(ctx, base+"stalled").Result()
	t.Logf("stalled set after kill+sleep: %v", stalledIDs)

	// mkq Worker with WithStalledInterval short enough to fire within
	// the test budget. The `defa` (default failed reason) field gets
	// set after maxStalledCount reclaim attempts; with WithMaxStalledCount(1)
	// the worker reclaims on the first scan and finalises straight to failed.
	var processed atomic.Int64
	mkqWorker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[interopPayload]) (any, error) {
		processed.Add(1)
		return nil, nil
	},
		mkq.WithIdlePollInterval(50*time.Millisecond),
		mkq.WithStalledInterval(500*time.Millisecond),
		mkq.WithMaxStalledCount(2), // allow one re-acquire so handler runs
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mkqWorker.Stop(context.Background()) })

	// Job either ends up processed (handler ran after stalled-recovery
	// re-acquired) or in failed (maxStalledCount tripped first). Either
	// outcome proves mkq's stalled scanner sees the BullMQ-orphaned
	// job; we accept both since the exact path depends on scan timing.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() > 0 {
			break
		}
		fz, _ := rdb.ZScore(ctx, base+"failed", addedJob.ID).Result()
		if fz > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if processed.Load() == 0 {
		fz, _ := rdb.ZScore(ctx, base+"failed", addedJob.ID).Result()
		if fz == 0 {
			activeNow, _ := rdb.LRange(ctx, base+"active", 0, -1).Result()
			stalledNow, _ := rdb.SMembers(ctx, base+"stalled").Result()
			waitNow, _ := rdb.LRange(ctx, base+"wait", 0, -1).Result()
			scTTL, _ := rdb.PTTL(ctx, base+"stalled-check").Result()
			t.Fatalf("mkq did not reclaim BullMQ-orphaned job after 20s: active=%v stalled=%v wait=%v stalled-check ttl=%v",
				activeNow, stalledNow, waitNow, scTTL)
		}
	}

	// Lock key must be released either way (no orphaned lock).
	exists, err := rdb.Exists(ctx, base+addedJob.ID+":lock").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, exists, "lock key should be released after stalled-recovery flow")

}
