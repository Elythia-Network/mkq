package mkq

import (
	"errors"
	"fmt"
	"time"
)

// ErrPriorityWithDelay is returned by Queue.Add when both WithPriority
// and WithDelay are supplied. BullMQ stores the priority in the HASH
// but the dispatch is delayed-only, so the priority would not affect
// dequeue ordering — surfacing the conflict avoids a silent footgun.
//
// A future PR may add a combined dispatch (delayed + prioritized) that
// removes this restriction.
var ErrPriorityWithDelay = errors.New("mkq: WithPriority cannot be combined with WithDelay")

// ErrUnrecoverable, when wrapped into a handler's returned error,
// instructs the worker to skip the retry path even if WithAttempts
// would otherwise allow another attempt. The job transitions straight
// to failed.
//
// This mirrors BullMQ's UnrecoverableError class for foreign-language
// workers: bull-board reports the same final-failed shape regardless
// of whether the rejection came from a JS Worker or mkq.
//
//	return fmt.Errorf("invalid input: %w", mkq.ErrUnrecoverable)
var ErrUnrecoverable = errors.New("mkq: unrecoverable error (skip retry)")

// ErrJobNotFound is returned by Queue.Get and the mutation methods
// (UpdateProgress / UpdateData / Log) when the BullMQ HASH for the
// target jobID is missing — the job either never existed, was
// removed via WithKeepCompleted(0), or expired from a separate
// retention policy.
var ErrJobNotFound = errors.New("mkq: job not found")

// ErrJobDetached is returned by Job mutation methods when the Job
// was constructed without a queue back-pointer (e.g. zero-valued
// for tests). All Job values produced by the mkq API — Queue.Add,
// Queue.Get, and the handler-dispatch path — carry the back-pointer
// automatically.
var ErrJobDetached = errors.New("mkq: job has no queue back-pointer (constructed outside mkq?)")

// ErrJobActive is returned by Queue.RemoveJob when the target job
// is currently locked (in active state). BullMQ's removeJob-2.lua
// returns 0 in this case; mkq surfaces it as ErrJobActive so callers
// can retry after the in-flight handler finishes.
var ErrJobActive = errors.New("mkq: job is active (locked) and cannot be removed")

// ErrJobIsScheduler is returned by Queue.RemoveJob when the job is
// the current iteration of a recurring schedule (jobId begins with
// "repeat:"). Removing such a job individually would orphan the
// schedule; use Queue.RemoveSchedule for the whole schedule instead.
// BullMQ's removeJob-2.lua returns -8 in this case.
var ErrJobIsScheduler = errors.New("mkq: job is owned by a scheduler; use RemoveSchedule")

// ErrJobNotInDelayed is returned by Queue.PromoteJob when the
// target job is not currently in the delayed ZSET (already promoted,
// already finished, or never delayed). BullMQ's promote-9.lua
// returns -3 in this case.
var ErrJobNotInDelayed = errors.New("mkq: job is not in the delayed state")

// ErrJobNotInExpectedState is returned by Queue.RetryJob when the
// target job is not in the source state requested via
// WithRetryFromState (default: "failed"). BullMQ's reprocessJob-7.lua
// returns -3 in this case.
var ErrJobNotInExpectedState = errors.New("mkq: job is not in the expected source state for retry")

// ErrDuplicateJob is returned by Queue.Add (alongside a Job[T]
// handle) when WithDeduplication / WithUnique suppresses the new
// enqueue because a prior job with the same dedup id is still
// within its TTL window. Callers can errors.Is-check this sentinel
// to distinguish "I just enqueued" from "the prior job is still
// pending."
//
// The returned Job[T] is a partial view: only Job.ID is the
// surviving job's identity (the lua returns the prior job's id
// here). Job.Data and Job.Timestamp are the *current call's*
// values — mkq cannot fish the prior payload out of Redis without
// a second round trip, so callers who need it should follow up with
// Queue.Get(jobID).
//
// Detection is best-effort: a narrow race between the pre-EVAL
// dedup-key GET and the EVAL itself can let a true duplicate slip
// through as a non-error response. The returned job ID is always
// usable regardless (the lua atomically enforces no-duplicate-job
// regardless of whether mkq surfaces the sentinel).
var ErrDuplicateJob = errors.New("mkq: job suppressed by deduplication; existing job returned")

// DelayedError, when returned (or wrapped) by a handler, moves the job
// back to the delayed set for Delay without consuming an attempt. The
// job keeps its ID and history, and is dispatched again once the delay
// elapses, with the same number of attempts left as before.
//
// Use it when the job itself is fine but now is the wrong time to run
// it — the downstream it talks to is known to be down, or asked to be
// left alone for a while — so that waiting does not eat into the retry
// budget meant for real failures.
//
//	if breakerOpen(host) {
//		return nil, fmt.Errorf("%s is down: %w", host, mkq.Delay(time.Until(nextProbe)))
//	}
//
// This mirrors BullMQ's DelayedError, which a JS handler throws after
// job.moveToDelayed: the transition is the same moveToDelayed script
// with its skip-attempt flag set, so nothing changes on the wire.
//
// A Delay of zero or less makes the job eligible again right away; it
// still goes through the delayed set and is never counted as a failure.
//
// If an error wraps both a DelayedError and ErrUnrecoverable, the delay
// wins: the job is put back rather than failed.
type DelayedError struct {
	Delay time.Duration
}

func (e *DelayedError) Error() string {
	return fmt.Sprintf("mkq: job delayed for %s", e.Delay)
}

// Delay returns a *DelayedError asking the worker to put the job back
// for d without consuming an attempt. See DelayedError.
func Delay(d time.Duration) error {
	return &DelayedError{Delay: d}
}
