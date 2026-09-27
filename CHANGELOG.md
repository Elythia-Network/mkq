# Changelog

All notable changes to mkq are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.4.0] - 2026-09-28

### Added

- `DelayedError` (and the `Delay` helper) lets a handler put its job
  back in the delayed set without consuming an attempt. The job keeps
  its ID and history and comes back once the delay elapses with the
  same attempts left as before; nothing is written to `failedReason`,
  and it is not counted as a failure.

  ```go
  if breakerOpen(host) {
      return nil, fmt.Errorf("%s is down: %w", host, mkq.Delay(time.Until(nextProbe)))
  }
  ```

  It is for "not now" rather than "failed": a downstream known to be
  down, or one that asked to be left alone for a while. Returning a
  plain error there would spend the retry budget on waiting and drop
  the job before the downstream recovers.

  This is BullMQ's `DelayedError`, which a JS handler throws after
  `job.moveToDelayed` (`skipAttempt: true`): the transition is the same
  `moveToDelayed` script with its skip-attempt flag set, so nothing
  changes on the wire.

## [1.3.0] - 2026-09-23

### Added

- `WithRetryDelayOverride` decides a failed job's retry delay, or
  declines and leaves it to the job's configured backoff. It is
  consulted for every backoff type, and for jobs carrying no backoff at
  all.

  It fills the gap the BullMQ-shaped hooks cannot: a queue on
  exponential backoff that should nonetheless honour a 429's
  `Retry-After` when the server sends one.

  ```go
  mkq.WithRetryDelayOverride(func(bc mkq.BackoffContext) (time.Duration, bool) {
      var ra *RetryAfterError
      if errors.As(bc.Err, &ra) {
          return min(ra.After, time.Hour), true
      }
      return 0, false // 他は設定どおり
  })
  ```

  Without it the only way to express that was to switch the queue to
  `backoff.type: "custom"`, which means owning the whole curve —
  returning 0 there is "retry immediately", not "use the default" — and
  which writes `custom` into the job's opts, so a BullMQ worker in
  another language reading the same queue would lose its backoff too.
  The override rewrites nothing on the wire.

  `WithBackoffStrategy` and `WithBackoffStrategyFunc` are unchanged and
  remain the BullMQ `settings.backoffStrategy` analogue for
  custom-typed jobs; the override composes with them rather than
  replacing them.

  A negative duration gives up and fails the job, as a custom strategy
  does. A panic is caught, logged, and treated as a decline — not as
  delay 0, which would quietly turn a considered backoff into a hot
  loop against whatever just failed.

  **BullMQ has no counterpart.** This is an API-surface extension: the
  delay still reaches `moveToDelayed` as plain integer milliseconds, so
  nothing about the wire format changes.

## [1.2.1] - 2026-09-23

### Changed

- `golang.org/x/sync` 0.21.0 -> 0.23.0. It backs the `singleflight` used
  for Lua script loading, and it is in the shipped `go.mod`, so it
  reaches consumers. The bench harness's `ioredis` moved to 6.0.0 as
  well; that one is test-only and ships in nothing.

### Fixed

- A schedule's job template now survives rescheduling whole. mkq's
  worker rebuilds the per-iteration options from the scheduler HASH
  when it queues the next run, and it only copied across the fields it
  had names for — so everything else was dropped from the second
  iteration onward.

  The visible case is a scheduler created by a **BullMQ TS** writer,
  since mkq's own `ScheduleOption`s cannot set these yet:

  ```
  TS queued iteration 1: {"attempts":3,"priority":5,
                          "backoff":{"type":"exponential","delay":1000},...}
  mkq queued iteration 2: {"jobId":"...","repeat":{"every":200},"delay":183}
  ```

  `attempts` going missing means the job stops retrying. `priority`
  going missing changes **where the job is stored** — the prioritized
  ZSET rather than the wait list — so it is a wire-level difference,
  not just a behavioural one.

  The template is now carried across as a whole rather than
  field-by-field, which is what BullMQ TS does (`{...opts, repeat}` in
  `job-scheduler.ts`). An option mkq has never heard of rides along
  instead of being silently discarded.

  `repeat.count` stays excluded on purpose: BullMQ recomputes it from
  the scheduler HASH's `ic` at re-upsert time, so carrying a stale
  Go-side value would race.

## [1.2.0] - 2026-09-23

### Added

- `WithScheduleKeepCompleted` / `WithScheduleKeepCompletedAge` and their
  failed-side counterparts put retention on the jobs a schedule creates.
  Until now `UpsertScheduleEvery` / `UpsertSchedulePattern` had no way to
  bound them, so a recurring job's completed and failed records grew
  without limit — a queue running a few hundred iterations a day
  accumulates tens of thousands of entries and the job HASHes to match.

  The retention lands in two places, because two different writers queue
  the iterations. It goes on the scheduler HASH's `opts` template, which
  is what a **BullMQ TS** worker reads when it schedules the next
  iteration, and on each iteration's own job opts, which is what
  `moveToFinished` consults. Put it in only one and the retention
  silently stops applying on whichever path you missed — mkq's worker
  now reads the template back for exactly this reason.

  The wire shapes match what BullMQ TS's `upsertJobScheduler` persists
  for the same options, pinned by an interop test that drives BullMQ's
  own scheduler and compares field for field.

  **On an existing backlog**, the two forms behave differently, and this
  is upstream BullMQ behaviour rather than anything mkq chooses:

  - `...Age` removes at most 1000 entries per completion (BullMQ's
    `keepJobs.limit` default), so a large backlog drains over several
    completions without any one of them blocking Redis.
  - `...KeepCompleted` / `...KeepFailed` remove **everything past the
    count in a single pass**, with no limit. Turning a count on against
    a set with tens of thousands of entries means one long-running Lua
    call. Prefer the age form when retro-fitting retention onto a queue
    that has been accumulating.

  Neither form runs on a timer: BullMQ evaluates retention only when a
  job of the same kind finishes, so nothing is trimmed until the next
  iteration completes.

### Fixed

- A retention age that rounds down to zero is no longer written to the
  wire. `WithKeepCompletedAge(500 * time.Millisecond)` truncates to
  `age: 0` at BullMQ's one-second resolution, and the two workers that
  might read that job disagreed about what it means: mkq's own finish
  path only sends `keepJobs.age` when it is positive, so nothing was
  trimmed, while BullMQ TS passes the object through untouched and its
  Lua then removes **everything** in the set scored at or before now —
  including the job that had just completed.

  The same job, the same options, opposite outcomes depending on which
  language processed it. The age is now left off entirely when it
  rounds to zero, which is what the godoc already promised. "Remove
  immediately" remains expressible as `WithKeepCompleted(0)`.

  Found while adding the scheduler-side options below, which would have
  inherited the same behaviour.

- The interop and bench harnesses declare `ioredis` again. **BullMQ 6
  moved it from a hard dependency to an optional peer dependency**, and
  npm does not install optional peers on its own, so `new Queue(...)`
  fails at construction with "could not load the optional 'ioredis'
  package".

  Both harnesses kept working only because their lockfiles still carried
  the entry npm had resolved back when BullMQ 5 depended on it outright.
  Regenerating either lockfile would have dropped it and broken the
  bull-board smoke test and the whole bench.

  It was removed in the first place because `bullmq@5.76.2` pinned an
  exact `ioredis`, which made the entry a copy of a decision it had no
  say in. That was true of 5 and stopped being true at 6.

  Test-only either way; no library code is affected.

## [1.1.1] - 2026-09-23

### Fixed

- `ListJobs` with `ascending=true` returned LIST-backed buckets (`wait`,
  `paused`, `active`) in the wrong order: the right window, but newest
  first inside it. Asking for a whole bucket ascending returned it
  descending.

  `getRanges-1.lua` reads those buckets with LRANGE and, for the
  ascending case, translates the range to negative indices. That picks
  the window from the old end; it does not reorder what comes back.
  **BullMQ TS reverses the result client-side** for exactly this reason
  (`src/classes/queue-getters.ts`, in `getRanges`); mkq did not, so the
  two disagreed on an API both implement, and mkq's own godoc promised
  an order it did not deliver.

  The Lua is vendored and untouched — the reversal belongs Go-side,
  which is where BullMQ puts it too. ZSET-backed buckets go through
  `ZRANGE` / `ZREVRANGE` and were already correct; they are left alone.

  Ordering now also holds across pages, so walking pages 0..n ascending
  walks the bucket oldest to newest. An interop test pins mkq's output
  against BullMQ TS's `Queue.getJobs` for the same arguments, and
  separately checks that BullMQ TS really does return oldest first —
  otherwise the comparison would pass with both sides broken.

  Callers that passed `ascending=false` are unaffected.


## [1.1.0] - 2026-09-23

### Upgrade notes

Three things in this release need a decision before upgrading.

- **Go 1.27 toolchain required.** The `go` directive is now `1.27.1`;
  an older toolchain cannot build a module that declares it. mkq's Go
  version tracks mk-go's, so the two move together.
- **`QueueCounts.Paused` counts something different.** It used to be the
  length of the `paused` list, which under BullMQ 6 is always empty. It
  now reports what is actually held back. Dashboards reading this field
  keep working; anything asserting "paused is 0 while jobs are parked"
  does not.
- **A backoff strategy returning a negative duration now fails the
  job** instead of retrying immediately. If a strategy used a negative
  return to mean "retry now", change it to return 0.

Wire format is unchanged except for pause, which is covered below.

### Added

- `Worker.Drain(ctx)` stops dequeueing and waits for the handlers that
  are already running to finish **without cancelling them**. `Stop`
  cancels them, which is the right thing when you need the process gone
  now, but it means a job cut mid-flight stays locked until the BullMQ
  lock expires and is then re-delivered by stalled detection.

  For a delivery worker that shows up as "the remote already had it, and
  we sent it again" on every deploy. `Drain` gives the in-flight work a
  bounded chance to land first; when its context expires it falls back
  to cancelling exactly as `Stop` does.

  Internally the run context and the parent of the per-job contexts are
  now separate. They used to be the same context, which made "stop
  taking new work" and "cancel what is running" inseparable.

  A `Drain` whose context expires cancels the handlers and returns
  straight away — the budget it was given is spent. Follow it with
  `Stop` on a fresh context to wait for them to unwind, and do not close
  the Redis client until that returns: the handler finalises its job
  after the cancellation, and a closed client turns the drain back into
  the redelivery it was meant to avoid.

- `Client.DiscoverQueues` enumerates the queues actually present in
  Redis under the client's key prefix, including ones mkq never
  `Define`'d — a queue created by a BullMQ worker in another language,
  or by another process. `Queues` answers "what does this process work
  on"; this answers "what is in this deployment", which is what a
  dashboard or an admin CLI needs.

  It scans for `{prefix}:*:meta`, the key BullMQ always writes, so the
  result is language-agnostic. A job id is an arbitrary string and can
  produce a key that ends the same way, so candidates carrying a `data`
  field — which job hashes have and queue metadata does not — are
  dropped in one pipelined round-trip. Cluster clients are scanned per
  master, since SCAN is per-node.

  SCAN is not free; this is an admin-path call, not a hot-path one.

- `WithBackoffStrategyFunc` registers a custom backoff that receives the
  job context — id, name, attempt count, the error the handler returned,
  and the backoff type — instead of only the attempt count.

  This closes a parity gap rather than adding a mkq-ism: BullMQ's own
  `settings.backoffStrategy` is called with
  `(attemptsMade, type, err, job)` (see
  `third_party/bullmq/src/types/backoff-strategy.ts`), while mkq's
  `CustomBackoffFunc` dropped the last three. **Without the error there
  is no way to honour an HTTP 429's `Retry-After`**, or to back off
  differently depending on why the attempt failed.

  `WithBackoffStrategy` keeps working unchanged. Registering both logs a
  warning at `Process` time and uses the context-aware one, since it can
  express everything the other can.

### Changed

- **BullMQ 6.** `third_party/bullmq` moves 5.76.2 -> 6.3.8 and the
  vendored Lua with it, so mkq's wire target is now BullMQ v6. The
  interop and bench harnesses move to `bullmq@6.3.8` to match.

  Only one thing changed on the wire: **pause no longer relocates
  jobs.** v5 renamed `wait` onto `paused` and routed jobs added during a
  pause into `paused` as well. v6 leaves everything in `wait` and lets
  the `meta.paused` flag alone gate the dequeue; `getTargetQueueList.lua`
  is gone. Key naming, id generation, job HASH fields, the events stream,
  schedulers, dedup, rate limiting and metrics are all unchanged.

  The gate itself is compatible in both directions — v5 and v6 both
  refuse to dequeue while `meta.paused` is set — so a v5 worker sharing
  a queue mkq paused still stops. What differs is where the backlog sits
  while paused.

  Three entry points changed arity and so changed filename:
  `moveToDelayed-12` -> `-11` and `reprocessJob-8` -> `-7` (both dropped
  the now-pointless `paused` key), `moveStalledJobsToWait-8` -> `-9`
  (gained the `repeat` key). Every entry point that kept its name kept
  its KEYS order.

- `Queue.Resume` now drains a legacy `paused` list in a loop. `pause-7`
  gained a return value: how many jobs are still parked. It moves at
  most 7000 per call so a long list cannot block Redis, which means one
  call is no longer enough — a queue that BullMQ 5 paused with a large
  backlog would have had everything past the first 7000 stranded.
  Bounded at 100 rounds (700k jobs) so a Lua that never reports zero
  cannot spin forever.

- Two BullMQ 6 changes ride along in the vendored Lua. mkq's own API
  touches neither, but both are visible to anything else sharing the
  queue:

  The legacy `debounced` event is gone from the events stream. v5
  emitted it alongside `deduplicated` with a `debounceId` field; v6
  emits only `deduplicated`. A foreign listener still subscribed to
  `debounced` stops receiving anything. mkq never emitted or consumed
  it.

  A job suppressed under `keepLastIfActive` now keeps the id it was
  given when it was added, instead of being re-created under a fresh
  one when the active job finishes. mkq does not expose that
  deduplication option (only `id` and `ttl`), so this only affects
  queues a BullMQ TS writer shares.

- `QueueCounts.Paused` and `ListJobs(JobBucketPaused)` answer "what is
  held back right now" rather than "what is in the `paused` list", which
  under v6 is always empty. While the queue is paused they report the
  `wait` contents, so those jobs are counted under both `Paused` and
  `Wait`. The exception is a queue paused by a BullMQ 5 writer: its
  backlog really is in the legacy `paused` list, and that count is
  reported as-is until `Resume` drains it. Both calls apply the same
  rule in the same order — if they disagreed, an admin UI would show
  "5 paused" over an empty table.

- Interop harness: `@bull-board/api` and `@bull-board/express` 6.13.1 ->
  9.10.1, and `express` 4.22.3 -> 5.2.1. The three move together because
  `@bull-board/express@9` depends on `express@^5.2.1` outright, not as a
  peer.

  `bullmq` stays at 5.76.2: bull-board 9 accepts `^5.56.0 || ^6.0.0`, so
  it does not drag the BullMQ pin along — and that pin has to keep
  matching `third_party/bullmq`, which is what the vendored Lua comes
  from.

  The `path-to-regexp` override is gone with it. It pinned 0.1.13 for
  express 4; express 5 uses a newer one and the pin would have held it
  back.

  Harness-only, but the bull-board smoke test is the thing that proves
  mkq's wire state renders in BullMQ's own admin UI, so the suite was run
  locally against the bump.

- Interop harness: dropped the top-level `ioredis` dependency. Nothing in
  the harness imports it — the five scripts import only `bullmq`,
  `express` and the two bull-board packages — and `bullmq@5.76.2` depends
  on `ioredis: 5.10.1` exactly, so the entry only ever mirrored a pin it
  had no say over. Raising it (as an update PR proposed) would have
  installed an unused second copy rather than upgrading anything.

- Go 1.27.1. The `go` directive moves with it, so **consumers need a
  1.27 toolchain**: a module declaring `go 1.27.1` cannot be built by an
  older one. mkq's Go version tracks mk-go's, so the two need to move
  together.

- A custom backoff strategy that returns a **negative** duration now
  stops the retries and fails the job, matching BullMQ, whose
  `settings.backoffStrategy` uses `-1` for exactly that
  (`third_party/bullmq/src/classes/job.ts`: `delay == -1 ? false : true`).
  Previously any non-positive return fell through to an immediate retry,
  so a strategy ported from TypeScript had its "give up" inverted into
  "resend now" and burned the remaining attempts back to back.

  This affects `WithBackoffStrategy` as well as the new option. A
  strategy that returned a negative duration meaning "retry immediately"
  should return 0 instead.

### Fixed

- `TestInterop_Wire_Priority` no longer depends on two jobs landing in
  different milliseconds. It compared `completed` ZSET scores, which are
  millisecond timestamps, so whenever both jobs finished inside the same
  millisecond the strict ordering assertion had nothing to stand on and
  the suite failed. It now reads the order of `active` events off the
  events stream, which is what priority actually governs — the job is
  dequeued first — and which keeps insertion order regardless of clock
  resolution.

  The flake predates this release; it surfaced while verifying the Go
  bump (1 failure in 3 runs on 1.27.1, 0 in 5 on 1.26.6) and is a
  property of the test, not of either toolchain.

- `Stop` cancels the in-flight handlers before it talks to Redis rather
  than after. The wake-up write it issues is bounded by a second, and
  "Redis is wedged" is the usual reason for reaching for `Stop` — the
  handlers should not sit uncancelled behind that round-trip.

- A panic inside a registered backoff strategy no longer takes the
  worker process down. It runs after `runHandler`'s recover has
  returned and the dispatch loop has none of its own, so the goroutine
  unwound and the job it held stayed locked in `active` until stalled
  recovery. The panic is now logged and the job retried immediately.

  No Redis wire format change: the delay still reaches Lua as a plain
  integer.

### Security

- Cleared the open Dependabot alerts.

  `go.opentelemetry.io/otel/sdk` 1.43.0 -> 1.46.0 (GHSA-8wmf-6v46-5gfg).
  The otel family moves together, so `otel` and `otel/trace` go to 1.46.0
  as well. It is a test-only import (`observability/oteladapter`'s tests),
  but it sits in `go.mod` all the same.

  In the interop harness, `express` 4.21.2 -> 4.22.3, which brings
  `qs` 6.16.0 (GHSA-4mjr-xmp4-gh2g, GHSA-x5fp-wj9c-mxmx) and
  `body-parser` 1.20.8 (GHSA-v422-hmwv-36x6); `brace-expansion` moves to
  2.1.7 (GHSA-rgw5-rvv9-x895, GHSA-3jxr-9vmj-r5cp, GHSA-mh99-v99m-4gvg).
  The `qs` override that pinned 6.15.2 is gone — express now resolves a
  fixed version on its own, and the pin only held it back.

  The harness is test-only and ships in no binary, but it runs in CI and
  a bull-board smoke test depends on express, so the interop suite was
  run locally against the bump before it landed.

## [1.0.8] - 2026-08-24

### Fixed

- `stacktrace` is now appended to on each failure instead of being
  overwritten with a single-element array. BullMQ TS accumulates one entry
  per attempt — that is why its consumers reverse the array to show the
  newest first. Overwriting meant **a retried job lost every earlier
  failure reason**, leaving only the last one.

### Added

- `JobState.AttemptsAt` records the start time of every failed attempt
  (unix milliseconds, oldest first), stored in the `mkqAttemptsAt` HASH
  field. **BullMQ has no equivalent** — it keeps no per-attempt timestamp,
  which is why admin UIs that try to plot retries have nothing to place
  them at. Unknown HASH fields are ignored by BullMQ and bull-board, so
  wire compatibility is preserved. Jobs that failed before this release do
  not have it and cannot be backfilled.

## [1.0.7] - 2026-08-24

### Added

- `JobState` now carries `Opts` (the BullMQ `opts` HASH field verbatim,
  as raw JSON) and `Delay` (milliseconds). `Queue.Get` already read the
  whole HASH and threw both away, so surfacing them costs no extra round
  trip. Admin UIs need `opts` to show a job's real `attempts` / `backoff`
  / `removeOnComplete` — reconstructing it from the fields mkq happens to
  know silently drops any key mkq does not model. `ListJobs` returns the
  same values.
- `Queue.GetJobLogs(ctx, jobID, start, end)` reads the log lines written
  by `Job.Log` / `Queue.AppendJobLog`, returning `JobLogs{Logs, Count}`.
  The write side existed since job mutations landed but there was no way
  to read them back, so an admin log tab had nothing to show. Mirrors
  BullMQ's `Queue.getJobLogs`: `count` is the full list length even when
  a sub-range is requested, and a missing job is indistinguishable from a
  job with no logs (both empty).

## [1.0.6] - 2026-08-18

### Fixed

- `Worker.Stop` could block forever when another worker was still
  running on the same queue. Stop wakes parked dispatchers by pushing to
  a Redis key, but it pushed to the queue-level `marker` — which every
  worker on that queue blocks on — so a surviving worker's dispatcher
  could consume the wake-up meant for the stopping one. A mk-go
  production instance failed to boot because of this: its autoscaler
  resized `inbox` from 16 workers down to 4, `Worker.Stop` never
  returned (the caller passed `context.Background()`), and the HTTP
  listener was never reached. Each Worker now owns a private wake ZSET
  and blocks on it alongside the shared marker, so a push always reaches
  the intended worker. The key is named with a uuid (`workerName` is
  caller-supplied and can collide) and is deleted once every dispatcher
  has exited. Measured: 32s (hitting a 20s context deadline) before,
  4ms after. (#78)
  - This is not a 1.0.5 regression — pushing to the shared marker dates
    from 1.0.4. It stayed hidden because dispatchers holding a delayed
    job sat in a precise sleep rather than on the marker; 1.0.5 moved
    them onto the marker and exposed it.

## [1.0.5] - 2026-08-18

### Fixed

- The idle backoff added in 1.0.4 was defeated by a single delayed job.
  When `tryOnce` reported the next delayed job's timestamp, the
  dispatcher took a precise-sleep path instead of parking on the marker,
  and that sleep was capped at `idlePollInterval` (100ms by default). So
  one job scheduled an hour out was enough to keep every dispatcher
  issuing `moveToActive` ten times a second — `awaitMarker` was never
  reached, so the wait never grew. With 8 workers, an otherwise idle
  queue went from 19 commands/s to **549**. Long delays now park on the
  marker (capped at 30s): a newly enqueued job still wakes the worker
  immediately via the marker push, so there is nothing to gain from
  polling. Sub-second delays keep the precise sleep — go-redis rounds a
  `BZPOPMIN` timeout up to one second, which would overshoot a 50ms
  retry backoff. (#76)

## [1.0.4] - 2026-08-18

### Changed

- Minimum Go version raised to 1.26 (matches mk-go's `go.mod`). CI now
  reads the version from `go.mod` via `setup-go`'s `go-version-file`,
  so the workflow and the module cannot drift apart.
- `WithIdlePollInterval` now sets the *floor* of the idle wait rather
  than a fixed period: an idle dispatcher doubles its wait on every
  empty poll, capped at 30s, and snaps back to the floor as soon as it
  processes a job. Job pickup latency is unchanged — the wait ends on
  the marker push that `addJob` performs — so callers do not need to
  retune the option. (#74)

### Fixed

- Idle workers no longer poll Redis once per second per dispatcher.
  go-redis rounds a `BZPOPMIN` timeout up to whole seconds, so every
  dispatcher woke and issued a `tryOnce` each second even with an empty
  queue, scaling with worker count. A mk-go production instance was
  issuing 774 commands/s against an idle queue where the equivalent
  BullMQ deployment issued 21.5. With the backoff, 8 workers idling for
  20s drop from 1,284 commands to 388. (#74)
- `Worker.Stop` is no longer bounded by `idlePollInterval`. Cancelling
  the context does not abort an in-flight `BZPOPMIN` — go-redis derives
  the read deadline from the block timeout and does not interrupt a
  read already issued — so shutdown waited out the remaining interval
  (7.78s measured at `interval=8s`). `Stop` now pokes the marker key to
  wake blocked dispatchers, bringing shutdown to single-digit
  milliseconds regardless of the interval. Each poked member is named
  per dispatcher: the marker is a sorted set, so repeating one member
  name collapses to a single entry and wakes only one waiter. (#74)

## [1.0.3] - 2026-06-23

### Added

- Public pause/resume API for BullMQ `Queue.pause()` / `Queue.resume()`
  parity. (#70)
  - `Queue.Pause` sets the `meta.paused` flag and atomically moves the
    `wait` list to `paused` (vendored `pause-7.lua`), so jobs already
    queued are parked rather than dropped. Jobs enqueued while paused
    also land in `paused` (no orphans); the pause is shared via Redis so
    every worker process honours it.
  - `Queue.Resume` clears the flag, returns parked jobs to `wait`, and
    pokes the marker ZSET so blocking workers wake immediately.
  - `Queue.IsPaused` reports the current state via `HEXISTS meta paused`.

## [1.0.2] - 2026-06-01

### Added

- Custom and jittered retry backoff strategies for BullMQ parity. (#67)
  - `FixedBackoffWithJitter` / `ExponentialBackoffWithJitter` apply
    BullMQ's jitter fraction (`0..1`); the value is persisted in the
    `opts.backoff` wire shape (`{type, delay, jitter}`) so foreign
    BullMQ workers honour it.
  - `CustomBackoff()` + `Worker.WithBackoffStrategy` expose BullMQ's
    `settings.backoffStrategy` path: an arbitrary `func(attemptsMade int)
    time.Duration` owns the formula, cap, and jitter. This lets mk-go
    reproduce Misskey's `httpRelatedBackoff` (`(2^n-1)*base`, capped at
    8h, plus 0-20% jitter) drop-in. (#66)

### Fixed

- (none)

## [1.0.1] - 2026-04-27

### Added

- BullMQ-compatible per-queue metrics (#59, #60). Enables the
  previously-disabled write path inside the vendored
  `moveToFinished-14.lua` so per-minute completed/failed buckets
  land in the BullMQ-spec keys (`bull:<q>:metrics:<target>` HASH +
  `...:data` LIST). bull-board / Misskey admin / mk-go admin
  charts that LRANGE the BullMQ key now see real data. Adopts
  BullMQ TS's API shape exactly:
  - `WithJobMetrics(maxDataPoints int) WorkerOption` — opt-in
    write path; mirrors BullMQ TS `WorkerOptions.metrics:
    { maxDataPoints }`.
  - `Queue[T].GetMetrics(ctx, kind, start, end) (QueueMetrics, error)`
    — atomic read via vendored `getMetrics-2.lua`.
  - `QueueMetrics{Meta: QueueMetricsMeta{Count, PrevTS, PrevCount},
    Data, Count}` matches BullMQ TS `Metrics` interface.
  - `ErrInvalidMetricsBucket` for non-completed/failed kinds.

### Notes

- This release is a strict additive minor under semver (no
  breaking changes); kept on the patch line per the project's
  early-stage `1.0.x` cadence.

## [1.0.0] - 2026-04-27

Initial stable release. Wire format and public API are stable from
this point onward; subsequent releases will only add features and
fix bugs without breaking existing callers.

### Added

#### Core (Phase 3)

- BullMQ-compatible `Queue.Add` with the three vendored entry-point
  Lua scripts (`addStandardJob-9` / `addDelayedJob-6` /
  `addPrioritizedJob-9`).
- Worker happy-path via `mkq.Process` — generic over the payload type
  with `Handler[T]`, lock heartbeat, panic recovery, graceful
  `Worker.Stop`.
- Retry and backoff: `WithAttempts`, `WithBackoff` with `FixedBackoff`
  / `ExponentialBackoff` constructors, `ErrUnrecoverable` sentinel.
- Job options: `WithDelay`, `WithPriority`, `WithLifo`, `WithJobID`.
- Retention controls: `WithKeepCompleted` / `WithKeepFailed` (count)
  and `WithKeepCompletedAge` / `WithKeepFailedAge` (age).
- Stalled-job detection (`moveStalledJobsToWait`) with
  `WithStalledInterval` / `WithMaxStalledCount`.
- Rate limiter: `WithRateLimit(max, duration)` honoured at
  `moveToActive` time; cross-worker via the BullMQ rate-limit ZSET.

#### BullMQ compat layer (Phase 4)

- Cross-language interop harness (Go test orchestrating Node + the
  real BullMQ TS library) — 29 subtests covering producer / consumer
  / mutation / admin / retry / stalled recovery / shared rate limit
  / inverse events / schedule options / dedup.
- bull-board admin-UI smoke test confirming queues populated by mkq
  render correctly through the BullMQ TS reader.
- `Queue.Get` + `JobState` for post-finalisation HASH read-side
  parity (returnvalue, processedOn, finishedOn, failedReason,
  stacktrace, attemptsMade, attemptsStarted, stalledCounter,
  processedBy).
- `QueueEvents.Subscribe` — typed subscriber for the BullMQ events
  stream (added / waiting / active / completed / failed / progress /
  stalled / drained / delayed / retries-exhausted / removed).
- Repeat scheduler — `Queue.UpsertScheduleEvery` (every-mode) and
  `Queue.UpsertSchedulePattern` (cron-pattern mode using the
  robfig/cron/v3 5-field Vixie parser); `Queue.RemoveSchedule`.
  Schedule options: `WithScheduleLimit`, `WithScheduleStartDate`,
  `WithScheduleEndDate`, `WithScheduleTimezone`,
  `WithScheduleImmediately`.
- Job mutation API: `Job.UpdateProgress` / `Job.UpdateData` /
  `Job.Log`, plus the out-of-band variants on `Queue`
  (`UpdateJobProgress` / `UpdateJobData` / `AppendJobLog`).
- Deduplication: `WithDeduplication(id, ttl)` and the
  asynq-compatible `WithUnique(id, ttl)` alias.

#### mk-go integration prereqs (Phase 5)

- Inspector API (read-only): `Client.Queues`, `Queue.Counts`,
  `Queue.ListJobs`, `Queue.Get`.
- Inspector API (mutations): `Queue.RemoveJob`, `Queue.DrainPending`
  (with `WithDrainDelayed`), `Queue.PromoteJob`, `Queue.RetryJob`.
  Five typed errors: `ErrJobNotFound`, `ErrJobActive`,
  `ErrJobIsScheduler`, `ErrJobNotInDelayed`,
  `ErrJobNotInExpectedState`.
- `WithJobName(string)` — per-task-type fan-out within a single
  queue.

#### 1.0 release prep (Phase 6)

- Performance: marker-based BZPopMin dispatch (replaces polling +
  fixed sleep), `moveToFinished` `fetchNext=true` (halves dispatch
  EVALSHAs), `sync.Pool` for msgpack encoders on the hot path.
- Benchmark harness vs BullMQ TS in `bench/` (Go + Node clients
  sharing the same Redis); end-to-end consume / produce / latency
  / memory metrics. mkq beats BullMQ TS on consume at
  concurrency ≥ 64; p99 latency mkq-favored 1.33–1.48× across all
  configurations.
- Observability: `Logger` / `Metrics` / `Tracer` / `Span` interfaces
  defaulting to noop; opt-in adapters in `observability/slogadapter`,
  `observability/promadapter`, `observability/oteladapter`. Five
  metrics and two spans emitted; five previously-silent operational
  sites (stalled-scan failures, NOSCRIPT reload, fetchNext shutdown
  race, BZPopMin pool-size warning, cron pattern parse failure)
  now surface through Logger.
- README, asynq migration guide
  (`docs/MIGRATING_FROM_ASYNQ.md`), godoc audit.

### Compatibility caveats

- Cluster mode: tested against go-redis cluster clients;
  cross-slot operations are avoided as in BullMQ.
- Pool sizing: BZPopMin holds a connection per worker slot. Set
  `Config.Redis.PoolSize` to at least `concurrency + 8`. mkq warns
  at startup when the configured pool is below this.
- Redis 6.0 and earlier: untested. The vendored Lua scripts use
  Redis 7-shaped command paths in places.

### Known limitations

- BullMQ flow producer (parent-child orchestration) — not
  implemented; the BullMQ TS feature is rarely needed in mk-go's
  workload and would touch wire-format territory we have not
  covered with interop tests yet.
- Sandboxed processors — not implemented; mkq runs handlers in the
  caller's goroutines.
- Pause / resume per-queue — not implemented; the underlying
  `paused` ZSET wire format is honoured by readers but mkq has no
  Go-side toggle yet.
- ioredis auto-pipelining behaviour cannot be matched by go-redis
  v9 (per-call EVALSHA), so 100k-job consume workloads see BullMQ
  TS pull ahead 1.24× at concurrency=16. Documented as the
  Redis-client-level gap in `bench/README.md`.

[Unreleased]: https://github.com/shiroha-a/mkq/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.4.0
[1.3.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.3.0
[1.2.1]: https://github.com/shiroha-a/mkq/releases/tag/v1.2.1
[1.2.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.2.0
[1.1.1]: https://github.com/shiroha-a/mkq/releases/tag/v1.1.1
[1.1.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.1.0
[1.0.8]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.8
[1.0.7]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.7
[1.0.6]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.6
[1.0.5]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.5
[1.0.4]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.4
[1.0.3]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.3
[1.0.2]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.2
[1.0.1]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.1
[1.0.0]: https://github.com/shiroha-a/mkq/releases/tag/v1.0.0
