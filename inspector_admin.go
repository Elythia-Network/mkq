package mkq

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/elythia-network/mkq/internal/lua"
)

// RemoveJob deletes a job and all its associated keys (logs, lock,
// dependency markers) from any state set it currently lives in.
// Equivalent to BullMQ's Job.remove.
//
// Returns:
//   - ErrJobActive       — job is locked (in active state); the worker
//     must finish or release the lock first.
//   - ErrJobIsScheduler  — job is the current iteration of a recurring
//     schedule; use RemoveSchedule on the parent
//     schedule instead of removing one fire.
//   - nil on success, including when the job had already been removed
//     by another path (BullMQ's removeJob is idempotent at the wire
//     level — it no-ops on missing HASHes rather than erroring).
func (q *Queue[T]) RemoveJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.RemoveJob,
		[]string{q.keys.Job(jobID), q.keys.Repeat()},
		jobID,
		"0", // shouldRemoveChildren=false (parent-child orchestration is out of mkq's scope today)
		q.keys.Base(),
	)
	if err != nil {
		return fmt.Errorf("mkq: removeJob: %w", err)
	}
	code, ok := res.(int64)
	if !ok {
		return fmt.Errorf("mkq: removeJob: unexpected result type %T", res)
	}
	switch code {
	case 1, 0:
		// 1 = removed; 0 = locked (active). The lua returns 0 when
		// isLocked succeeds; mkq surfaces it as ErrJobActive.
		if code == 0 {
			return ErrJobActive
		}
		return nil
	case -8:
		return ErrJobIsScheduler
	default:
		return fmt.Errorf("mkq: removeJob returned error code %d", code)
	}
}

// DrainOption customises a DrainPending call.
type DrainOption func(*drainConfig)

type drainConfig struct {
	includeDelayed bool
}

// WithDrainDelayed extends DrainPending to also remove jobs from the
// delayed ZSET. Default false: only wait / paused / prioritized are
// drained. Scheduler-owned delayed jobs (rjk-stamped) are preserved
// regardless — drain-5.lua's preflight skips any delayed job whose ID
// matches a current scheduler iteration.
func WithDrainDelayed(b bool) DrainOption {
	return func(c *drainConfig) { c.includeDelayed = b }
}

// DrainPending removes every queued job that hasn't been picked up
// yet. Active jobs (in flight under a worker lock) and terminal
// jobs (completed / failed) are left alone — DrainPending is the
// administrative "cancel everything pending" knob, not a wipe.
//
// Buckets drained:
//   - wait, paused (LIST-backed)
//   - prioritized (ZSET-backed)
//   - delayed (ZSET-backed) only when WithDrainDelayed(true) is set
//
// Scheduler iterations whose IDs match an entry in the repeat ZSET
// are preserved even when WithDrainDelayed(true) is set; the lua
// computes the protected set from the repeat ZSET before draining.
func (q *Queue[T]) DrainPending(ctx context.Context, opts ...DrainOption) error {
	cfg := drainConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	delayedFlag := "0"
	if cfg.includeDelayed {
		delayedFlag = "1"
	}

	_, err := q.client.scripts.Run(
		ctx,
		lua.Drain,
		[]string{
			q.keys.Wait(),
			q.keys.Paused(),
			q.keys.Delayed(),
			q.keys.Prioritized(),
			q.keys.Repeat(),
		},
		q.keys.Base(),
		delayedFlag,
	)
	// drain-5.lua はステータスコードを返さない。go-redis は何も返さない
	// EVALSHA を redis.Nil として surface するので、これは成功と扱う。
	// 実エラー (NOSCRIPT 後の reload 失敗等) はそのまま伝播。
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("mkq: drain: %w", err)
	}
	return nil
}

// PromoteJob moves a delayed job to the wait list immediately,
// bypassing its scheduled fire time. Equivalent to BullMQ's
// Job.promote and asynq's Inspector.RunTask for delayed jobs.
//
// Returns ErrJobNotInDelayed when the job is not in the delayed
// ZSET (already promoted, already finished, or never delayed). For
// retry-state jobs (failed / completed re-enqueue) use RetryJob.
func (q *Queue[T]) PromoteJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	res, err := q.client.scripts.Run(
		ctx,
		lua.Promote,
		[]string{
			q.keys.Delayed(),
			q.keys.Wait(),
			q.keys.Paused(),
			q.keys.Meta(),
			q.keys.Prioritized(),
			q.keys.Active(),
			q.keys.PriorityCounter(),
			q.keys.Events(),
			q.keys.Marker(),
		},
		q.keys.Base(),
		jobID,
	)
	if err != nil {
		return fmt.Errorf("mkq: promote: %w", err)
	}
	code, ok := res.(int64)
	if !ok {
		return fmt.Errorf("mkq: promote: unexpected result type %T", res)
	}
	switch code {
	case 0:
		return nil
	case -3:
		return ErrJobNotInDelayed
	default:
		return fmt.Errorf("mkq: promote returned error code %d", code)
	}
}

// RetryOption customises a RetryJob call.
type RetryOption func(*retryConfig)

type retryConfig struct {
	fromState JobBucket
	resetAtm  bool
	resetAts  bool
}

// WithRetryFromState picks the source bucket the job is currently in.
// Default JobBucketFailed; pass JobBucketCompleted for the rare case
// of re-enqueueing a successful job (matches BullMQ Job.retry's
// behaviour for both failed and completed sources).
func WithRetryFromState(state JobBucket) RetryOption {
	return func(c *retryConfig) { c.fromState = state }
}

// WithResetAttempts resets the BullMQ `atm` (attemptsMade) and `ats`
// (attemptsStarted) HASH counters before re-enqueueing. Default true
// (matches BullMQ's Job.retry default and asynq's RunTask).
func WithResetAttempts(reset bool) RetryOption {
	return func(c *retryConfig) {
		c.resetAtm = reset
		c.resetAts = reset
	}
}

// RetryJob re-enqueues a job from the failed (or completed) ZSET
// back into wait. Equivalent to BullMQ's Job.retry and the second
// half of asynq's Inspector.RunTask (the first half is PromoteJob
// for delayed jobs).
//
// Returns:
//   - ErrJobNotFound          — the job HASH is missing entirely
//     (already removed / retention-expired).
//   - ErrJobNotInExpectedState — the job exists but isn't in the
//     source bucket (e.g. RetryJob from
//     "failed" but the job is in "wait").
func (q *Queue[T]) RetryJob(ctx context.Context, jobID string, opts ...RetryOption) error {
	if jobID == "" {
		return fmt.Errorf("mkq: jobID must be non-empty")
	}
	cfg := retryConfig{
		fromState: JobBucketFailed,
		resetAtm:  true,
		resetAts:  true,
	}
	for _, o := range opts {
		o(&cfg)
	}

	var stateKey, propVal, prevState string
	switch cfg.fromState {
	case JobBucketFailed:
		stateKey = q.keys.Failed()
		propVal = "failedReason"
		prevState = "failed"
	case JobBucketCompleted:
		stateKey = q.keys.Completed()
		propVal = "returnvalue"
		prevState = "completed"
	default:
		return fmt.Errorf("mkq: RetryJob source state %q must be JobBucketFailed or JobBucketCompleted", cfg.fromState)
	}

	resetAtm, resetAts := "0", "0"
	if cfg.resetAtm {
		resetAtm = "1"
	}
	if cfg.resetAts {
		resetAts = "1"
	}

	res, err := q.client.scripts.Run(
		ctx,
		lua.ReprocessJob,
		[]string{
			q.keys.Job(jobID),
			q.keys.Events(),
			stateKey,
			q.keys.Wait(),
			q.keys.Meta(),
			// BullMQ 6 で paused key が KEYS から外れた。pause 中でもジョブは
			// wait に入るので、行き先を分ける必要が無くなったため。
			q.keys.Active(),
			q.keys.Marker(),
		},
		jobID,
		"LPUSH", // FIFO push (matches BullMQ default; LIFO retry isn't a documented BullMQ feature)
		propVal,
		prevState,
		resetAtm,
		resetAts,
	)
	if err != nil {
		return fmt.Errorf("mkq: reprocessJob: %w", err)
	}
	code, ok := res.(int64)
	if !ok {
		return fmt.Errorf("mkq: reprocessJob: unexpected result type %T", res)
	}
	switch code {
	case 1:
		return nil
	case -1:
		return ErrJobNotFound
	case -3:
		return ErrJobNotInExpectedState
	default:
		return fmt.Errorf("mkq: reprocessJob returned error code %d", code)
	}
}

// Pause stops the queue from handing jobs to workers, mirroring
// BullMQ's Queue.pause(). It sets the `meta.paused` flag and drops the
// marker; the queued jobs stay exactly where they are.
//
// **BullMQ 6 で pause はジョブを動かさなくなった。** v5 までは
// `wait` を `paused` へ RENAME して退避し、pause 中の新規ジョブも
// `paused` に入れていた。6 ではどちらも `wait` のままで、gate は
// `meta.paused` のフラグだけが持つ。退避が無いぶん pause / resume の
// コストがキューの長さに依存しない。
//
// The pause is global to the queue and shared via Redis, so every
// worker process bound to the same queue honours it (the gate lives in
// moveToActive's Lua, not in any single client). In-flight jobs already
// locked by a worker are allowed to finish — Pause only blocks new
// fetches.
//
// Equivalent to BullMQ pause-7.lua with ARGV "paused". Idempotent:
// pausing an already-paused queue is a no-op at the wire level.
func (q *Queue[T]) Pause(ctx context.Context) error {
	return q.pauseResume(ctx, q.keys.Wait(), q.keys.Paused(), "paused")
}

// Resume re-enables job processing on a paused queue, mirroring
// BullMQ's Queue.resume(). It clears the `meta.paused` flag and pokes
// the marker ZSET so blocking workers wake immediately instead of
// waiting out their BRPOPLPUSH timeout.
//
// A queue paused by a BullMQ 5 worker (or by mkq before it followed
// BullMQ 6) still has jobs parked in the legacy `paused` list. Resume
// drains those back into `wait`, in batches, repeating until none are
// left — the Lua moves at most 7000 per call so a long list cannot block
// Redis.
//
// If that drain hits its round cap Resume returns an error, but the
// queue is already running by then: the Lua clears `meta.paused` on its
// first call. Calling Resume again continues the drain from where it
// stopped.
//
// Equivalent to BullMQ pause-7.lua with ARGV "resumed". Idempotent:
// resuming a queue that is not paused is a no-op at the wire level.
func (q *Queue[T]) Resume(ctx context.Context) error {
	return q.pauseResume(ctx, q.keys.Paused(), q.keys.Wait(), "resumed")
}

// pauseResume runs the vendored pause-7.lua. source/target are the
// LIST keys to rename (wait->paused for pause, paused->wait for
// resume); event is the BullMQ stream event ("paused" or "resumed")
// that also selects the branch inside the Lua.
// legacyDrainRounds caps how many times Resume re-runs to empty a
// legacy `paused` list. The Lua moves up to 7000 jobs per call, so this
// covers 700k parked jobs — far past anything a queue should be holding,
// and bounded so a Lua that never reports zero cannot spin forever.
const legacyDrainRounds = 100

func (q *Queue[T]) pauseResume(ctx context.Context, source, target, event string) error {
	for round := 0; ; round++ {
		remaining, err := q.runPauseScript(ctx, source, target, event)
		if err != nil {
			return err
		}
		if remaining <= 0 {
			return nil
		}
		// BullMQ 5 が残した paused リストを吸い出している最中。
		// 1 回あたり 7000 件までなので、空になるまで繰り返す。
		if round+1 >= legacyDrainRounds {
			return fmt.Errorf("mkq: %s: %d jobs still parked in the legacy paused list after %d rounds",
				event, remaining, legacyDrainRounds)
		}
	}
}

// runPauseScript runs pause-7.lua once and reports how many jobs are
// still sitting in the legacy `paused` list.
func (q *Queue[T]) runPauseScript(ctx context.Context, source, target, event string) (int64, error) {
	res, err := q.client.scripts.Run(
		ctx,
		lua.Pause,
		// pause-7.lua KEYS:
		//   1 source list, 2 target list, 3 meta, 4 prioritized,
		//   5 events, 6 delayed, 7 marker
		[]string{
			source,
			target,
			q.keys.Meta(),
			q.keys.Prioritized(),
			q.keys.Events(),
			q.keys.Delayed(),
			q.keys.Marker(),
		},
		event,
		"1", // ARGV[2]: emit the paused / resumed event
	)
	// 返り値は legacy paused リストの残件数。BullMQ 5 の pause-7 は何も
	// 返さず、go-redis はそれを redis.Nil として surface していたので、
	// 5 系の script が残っている環境でも成功扱いにする。
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("mkq: %s: %w", event, err)
	}
	remaining, _ := res.(int64)
	return remaining, nil
}

// IsPaused reports whether the queue is currently paused, mirroring
// BullMQ's Queue.isPaused(). It checks for the `paused` field on the
// `meta` HASH (HEXISTS) — an empty `paused` list and an unpaused queue
// are distinct states, so the flag, not the list, is authoritative.
func (q *Queue[T]) IsPaused(ctx context.Context) (bool, error) {
	paused, err := q.client.rdb.HExists(ctx, q.keys.Meta(), "paused").Result()
	if err != nil {
		return false, fmt.Errorf("mkq: isPaused: %w", err)
	}
	return paused, nil
}
