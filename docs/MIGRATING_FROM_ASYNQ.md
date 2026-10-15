# Migrating from asynq to mkq

mkq is designed as a drop-in replacement for asynq when the
underlying queue needs to interoperate with BullMQ — for example,
when an admin UI like bull-board, or workers in another language,
need to share queues with the Go side.

This guide assumes a working asynq deployment and walks through the
mechanical replacements. mkq's API is intentionally close to
asynq's where the semantics line up, so most call sites change a
package import and an option name.

## When to migrate

Reach for mkq when **any** of the following apply:

- You operate a polyglot stack and want a single Redis-backed queue
  visible to Go (mkq), Node (BullMQ), and admin UIs (bull-board) —
  asynq's wire format is internal-only and cannot be read by
  cross-language tools.
- You want to plug an existing BullMQ deployment (Misskey, etc.)
  into a Go service without writing a translation layer.
- You need BullMQ-only features that asynq does not expose, such
  as recurring schedulers (every / cron) with limit + endDate, the
  job-mutation API (`UpdateProgress` / `Log`), or the QueueEvents
  stream.

If you only need a Go-only queue and have no BullMQ in your stack,
asynq is fine — there is no functional reason to migrate purely for
its own sake.

## Conceptual mapping

| Asynq concept | mkq equivalent | Notes |
|---|---|---|
| `asynq.Client` | `mkq.Client` | Constructed via `mkq.NewClient(ctx, mkq.Config{...})`. |
| `asynq.NewTask("name", payload)` | `mkq.Define[T](client, "queue")` + `Queue.Add(ctx, payload, ...)` | mkq is generic over the payload type — no manual `[]byte` marshal. |
| `client.Enqueue(task, opts...)` | `queue.Add(ctx, payload, opts...)` | Same shape; options listed below. |
| `asynq.Server` + `mux` | `mkq.Process(queue, handler)` | One handler per queue. Use multiple `Process` calls if you have multiple queues; use a switch on `job.Name` for per-task-type fan-out within one queue. |
| `srv.Run(mux)` | (n/a — `mkq.Process` returns immediately, runs in goroutines) | mkq does not block the calling goroutine; call `worker.Stop(ctx)` to drain. |
| `asynq.Inspector` | `mkq.Queue` (`Counts`, `ListJobs`, `Get`, `RemoveJob`, `RetryJob`, `PromoteJob`, `DrainPending`) | Methods land on the typed `Queue` itself, not a separate Inspector handle. |
| `*asynq.Task.ID` | `*mkq.Job[T].ID` | Plain string in both. |
| `task.ResultWriter()` | `Job.UpdateProgress` / `Job.UpdateData` / `Job.Log` | mkq mirrors BullMQ's job-mutation API, which has overlapping but not identical semantics. |
| `asynq.MaxRetry(n)` | `mkq.WithAttempts(n)` | Same meaning. |
| `asynq.Retention(d)` | `mkq.WithKeepCompletedAge(d)` + `mkq.WithKeepFailedAge(d)` | mkq splits completed and failed retention because BullMQ does. |
| `asynq.Unique(d)` | `mkq.WithUnique(id, d)` | mkq requires an explicit dedup id. The id can be a hash of the payload — see the producer example below. |
| `asynq.ProcessIn(d)` | `mkq.WithDelay(d)` | Same. |
| `asynq.ProcessAt(t)` | `mkq.WithDelay(time.Until(t))` | mkq encodes delay relative to enqueue time, like BullMQ. |
| `asynq.Queue("critical")` | `mkq.Define[T](client, "critical")` | The queue name lives on the typed handle, not on each Add. |
| `asynq.Group("...")` (Pro) | (no direct equivalent) | Group aggregation is asynq-specific. |
| `asynq.PeriodicTaskManager` | `Queue.UpsertScheduleEvery` / `UpsertSchedulePattern` | mkq schedules live in Redis (BullMQ-compatible), not in a Go-side manager. |

## Producer

### Asynq

```go
client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
defer client.Close()

payload, _ := json.Marshal(EmailPayload{To: "a@example.com"})
task := asynq.NewTask("email:welcome", payload)

_, err := client.Enqueue(task,
    asynq.MaxRetry(3),
    asynq.ProcessIn(5*time.Second),
    asynq.Unique(time.Hour),
)
```

### mkq

```go
client, _ := mkq.NewClient(ctx, mkq.Config{
    Redis: redis.UniversalOptions{Addrs: []string{"localhost:6379"}},
})
defer client.Close()

queue := mkq.Define[EmailPayload](client, "email")

_, err := queue.Add(ctx,
    EmailPayload{To: "a@example.com"},
    mkq.WithJobName("email:welcome"),       // optional, used for per-task-type dispatch in Process
    mkq.WithAttempts(3),
    mkq.WithDelay(5*time.Second),
    mkq.WithUnique("welcome:a@example.com", time.Hour),
)
```

Differences worth noting:

- mkq is generic over the payload — no manual `json.Marshal`.
- `WithJobName` is the BullMQ-side `name` field. It is optional;
  default is the queue name. Use it to fan out one queue across
  multiple task types in `Process`.
- `WithUnique` requires an explicit dedup id (asynq derives one
  from the payload). The id is your call — common choices: hash of
  the payload, the user/inbox id, a logical operation key.

## Consumer

### Asynq

```go
srv := asynq.NewServer(
    asynq.RedisClientOpt{Addr: "localhost:6379"},
    asynq.Config{Concurrency: 16},
)

mux := asynq.NewServeMux()
mux.HandleFunc("email:welcome", func(ctx context.Context, t *asynq.Task) error {
    var p EmailPayload
    _ = json.Unmarshal(t.Payload(), &p)
    return send(p)
})

if err := srv.Run(mux); err != nil { log.Fatal(err) }
```

### mkq

```go
queue := mkq.Define[EmailPayload](client, "email")

worker, _ := mkq.Process(queue,
    func(ctx context.Context, job *mkq.Job[EmailPayload]) (any, error) {
        switch job.Name {
        case "email:welcome":
            return nil, send(job.Data)
        case "email:reset":
            return nil, sendReset(job.Data)
        default:
            return nil, fmt.Errorf("unknown task: %s", job.Name)
        }
    },
    mkq.WithConcurrency(16),
)

defer worker.Stop(context.Background())
```

`Process` returns immediately; the worker runs in goroutines. Call
`Stop` to drain. There is no `srv.Run(mux)` blocker — wire the
graceful shutdown to your service lifecycle of choice (e.g. an
errgroup or signal handler).

## Retry and backoff

### Asynq

```go
client.Enqueue(task,
    asynq.MaxRetry(5),
    asynq.RetryUntil(time.Now().Add(1*time.Hour)),
)
```

Asynq backoff is fixed exponential by default; override via
`asynq.Config.RetryDelayFunc`.

### mkq

```go
queue.Add(ctx, payload,
    mkq.WithAttempts(5),
    mkq.WithBackoff(mkq.ExponentialBackoff(time.Second)), // or FixedBackoff(d)
)
```

Backoff strategy lives on each Add (BullMQ semantics) rather than
the server config. To force a non-retryable error from inside the
handler:

```go
return nil, fmt.Errorf("payment declined: %w", mkq.ErrUnrecoverable)
```

## Unique / deduplication

### Asynq

```go
client.Enqueue(task, asynq.Unique(time.Hour)) // id derived from payload
```

### mkq

```go
queue.Add(ctx, payload, mkq.WithUnique("unique-id", time.Hour))
// returns ErrDuplicateJob (with the existing job ptr) on hit
```

If you want the asynq-style "hash of payload" behavior, hash the
payload yourself:

```go
h := sha256.Sum256(mustJSON(payload))
mkq.WithUnique(hex.EncodeToString(h[:]), time.Hour)
```

## Recurring schedules

Asynq exposes `PeriodicTaskManager` (Go-side manager that re-enqueues
on a tick). mkq stores schedules in Redis using BullMQ's wire format,
so they survive Go process restarts and are visible to bull-board.

### mkq

```go
// Every 30 minutes.
queue.UpsertScheduleEvery(ctx, "metrics-cron", 30*time.Minute, MetricsPayload{})

// Cron pattern (Vixie 5-field, parsed via robfig/cron/v3).
queue.UpsertSchedulePattern(ctx, "midnight-summary", "0 0 * * *", SummaryPayload{},
    mkq.WithScheduleTimezone("Asia/Tokyo"),
    mkq.WithScheduleLimit(30),
    mkq.WithScheduleEndDate(time.Now().Add(30*24*time.Hour)),
)

// Stop a schedule.
queue.RemoveSchedule(ctx, "metrics-cron")
```

There is no asynq equivalent to `WithScheduleLimit` (cap N
iterations) or `WithScheduleEndDate` (stop at T). These are BullMQ
features.

## Inspector

### Asynq

```go
inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: ...})
inspector.GetQueueInfo("email")
inspector.ListPendingTasks("email")
inspector.DeleteTask("email", taskID)
```

### mkq

```go
counts, _   := queue.Counts(ctx)                                       // QueueCounts{Wait, Active, Completed, Failed, Delayed, Prioritized, Paused}
listed, _   := queue.ListJobs(ctx, mkq.JobBucketWait, 0, 99, true)     // ascending=true
job, st, _  := queue.Get(ctx, listed[0].Job.ID)                        // *Job[T] + *JobState

queue.RetryJob(ctx, job.ID)                                            // failed -> wait (prioritized if the job has a priority)
queue.PromoteJob(ctx, job.ID)                                          // delayed -> wait
queue.RemoveJob(ctx, job.ID)
queue.DrainPending(ctx, mkq.WithDrainDelayed(true))
_ = st
```

The Inspector methods live on the typed `Queue` directly. There is
no separate Inspector handle.

## Job mutation from inside a handler

```go
mkq.Process(queue, func(ctx context.Context, job *mkq.Job[Upload]) (any, error) {
    for i, chunk := range job.Data.Chunks {
        upload(chunk)
        _ = job.UpdateProgress(ctx, float64(i+1)/float64(len(job.Data.Chunks)))
        _ = job.Log(ctx, fmt.Sprintf("chunk %d done", i))
    }
    return nil, nil
})
```

Asynq has `task.ResultWriter()` for return-value streaming; mkq's
job-mutation API is BullMQ-shaped and emits `progress` events on
QueueEvents (for live UI updates).

## What does not migrate

mkq deliberately does not implement these asynq-specific features:

- **Group aggregation** (`asynq.Group("...")`). asynq-Pro feature
  with no BullMQ analogue.
- **Server-side scheduler manager** (`PeriodicTaskManager`). mkq's
  schedules live in Redis — see Recurring Schedules above.
- **Per-server middleware** (`asynq.MiddlewareFunc`). Wrap your
  handler manually if you need this; mkq does not provide a chain.
- **Result writer for streaming partial outputs.** Use
  `Job.UpdateData` to overwrite the data field with intermediate
  state if you need similar functionality.

## Driver swap in mk-go

mk-go's queue layer is an `asynq` interface today. The migration
plan replaces `AsynqDriver` with a new `MkqDriver` that implements
the same interface against this library. The mk-go-side PR is
tracked separately; on the mkq side, the API is stable enough that
the driver implementation has no remaining blockers.

If you are integrating mkq into a code base that previously used
asynq, the mechanical recipe is:

1. Replace `asynq.NewClient(...)` with `mkq.NewClient(ctx, ...)`.
2. Replace `asynq.NewTask("name", payload)` + `client.Enqueue(...)`
   with `queue.Add(ctx, payload, mkq.WithJobName("name"), ...)`.
3. Replace the `asynq.Server` + `mux` setup with one or more
   `mkq.Process(queue, handler)` calls; switch on `job.Name`
   inside the handler if you previously used a mux.
4. Translate options per the table at the top of this doc.
5. Replace `asynq.Inspector` calls with the equivalent
   `Queue.Counts` / `Queue.ListJobs` / `Queue.Get` /
   `Queue.RemoveJob` / `Queue.RetryJob` / `Queue.PromoteJob` /
   `Queue.DrainPending` calls.

For ops, the largest change is that bull-board (or any BullMQ-aware
admin UI) now sees the Go-side queues. asynq's web UI no longer
applies — but bull-board is more capable, so this is generally a
gain.
