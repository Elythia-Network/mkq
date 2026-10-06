package mkq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/elythia-network/mkq/internal/keys"
	"github.com/elythia-network/mkq/internal/lua"
	"github.com/elythia-network/mkq/internal/proto"
)

// Queue is a typed handle to a BullMQ-compatible queue.
//
// Construct one via Define[T]; reuse the returned pointer for the
// lifetime of the Client.
type Queue[T any] struct {
	client *Client
	name   string
	keys   keys.Builder
}

// Define returns a Queue handle for name. Calling Define twice with the
// same name is safe; both handles operate on identical Redis state.
//
// Side effect: Define writes mkq's library version into the BullMQ
// `meta` HASH via HSETNX. The non-overwriting semantics matter for
// queues already initialised by a foreign client (BullMQ TS would
// have written `bullmq:<semver>`); we only stamp `mkq:<semver>` when
// the field is missing.
func Define[T any](c *Client, name string, _ ...QueueOption) *Queue[T] {
	q := &Queue[T]{
		client: c,
		name:   name,
		keys:   keys.New(c.keyPrefix, name),
	}
	q.bootstrap()
	return q
}

// bootstrap fires the two best-effort side effects Define needs:
// stamp meta.version (HSETNX, BullMQ-compatible) and add this queue
// to the Client.Queues registry SET. Both are pipelined so Define's
// worst-case latency stays within a single 2s timeout instead of
// stacking — a single Redis round-trip carries both writes.
//
// Errors are intentionally dropped: a Redis hiccup here just means
// the version stays unstamped (interop test will catch the gap on
// the next bull-board read) or Queues() under-reports until the
// next Define for the same name re-runs the SADD.
func (q *Queue[T]) bootstrap() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pipe := q.client.rdb.Pipeline()
	pipe.HSetNX(ctx, q.keys.Meta(), "version", versionTag())
	pipe.SAdd(ctx, q.client.queueRegistryKey(), q.name)
	_, _ = pipe.Exec(ctx)
}

// Name returns the queue name as passed to Define.
func (q *Queue[T]) Name() string { return q.name }

// Add enqueues a new job with the given payload and options. The
// returned Job mirrors the wire-level state Lua just wrote to Redis.
//
// Add chooses one of three vendored Lua scripts based on the supplied
// options:
//
//   - WithDelay rounds to >= 1ms → addDelayedJob-6
//   - WithPriority > 0           → addPrioritizedJob-9
//   - otherwise                  → addStandardJob-9
//
// Combining WithPriority and WithDelay returns ErrPriorityWithDelay.
func (q *Queue[T]) Add(ctx context.Context, payload T, opts ...AddOption) (job *Job[T], err error) {
	ctx, span := q.client.tracer.Start(ctx, SpanQueueAdd, slog.String(AttrQueue, q.name))
	defer func() {
		// dedup hit は trace 上の "error" として扱わない (dashboard の
		// error rate を汚さないため)。SetError 経由ではなく
		// `mkq.dedup.hit=true` attr で記録に切り替える。dedup branch 側
		// で attr セット済み。
		if err != nil && !errors.Is(err, ErrDuplicateJob) {
			span.SetError(err)
		}
		span.End()
	}()

	cfg := addConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	delayMs := cfg.delay.Milliseconds()
	if delayMs > 0 && cfg.priority > 0 {
		return nil, ErrPriorityWithDelay
	}
	if cfg.dedupSet && cfg.dedupID == "" {
		return nil, fmt.Errorf("mkq: WithDeduplication / WithUnique id must be non-empty")
	}
	if cfg.jobNameSet && cfg.jobName == "" {
		return nil, fmt.Errorf("mkq: WithJobName name must be non-empty")
	}

	dataJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("mkq: marshal payload: %w", err)
	}

	timestampMs := time.Now().UnixMilli()

	jobName := q.name
	if cfg.jobName != "" {
		jobName = cfg.jobName
	}
	span.SetAttrs(slog.String(AttrJobName, jobName))

	args := proto.AddArgs{
		Prefix:    q.keys.Base(),
		CustomID:  cfg.jobID,
		Name:      jobName,
		Timestamp: timestampMs,
	}
	// dedup key を AddArgs[9] にも渡しておかないと lua 側の
	// deduplicateJob が SET 対象のキーを知らない。空文字なら
	// EncodeAddArgs が nil として詰めるので非 dedup 用途には影響なし。
	var dedupKey string
	if cfg.dedupID != "" {
		dedupKey = q.keys.Dedup(cfg.dedupID)
		args.DeduplicationKey = dedupKey
	}
	argsBytes, err := proto.EncodeAddArgs(args)
	if err != nil {
		return nil, fmt.Errorf("mkq: encode args: %w", err)
	}

	optsBytes, err := proto.EncodeAddOpts(toProtoOpts(cfg, delayMs))
	if err != nil {
		return nil, fmt.Errorf("mkq: encode opts: %w", err)
	}

	// dedup hit を Go 側で検出するため、EVAL 直前の dedup key の値を
	// 取っておく。lua は hit / miss どちらでも返り値が string jobID
	// なので、pre-GET の結果と返り値を比較して hit を識別する。
	// pre-GET と EVAL の間の race は下流ドキュメント通り best-effort。
	var preDedupID string
	if dedupKey != "" {
		preDedupID, _ = q.client.rdb.Get(ctx, dedupKey).Result()
	}

	scriptName, ks := q.dispatch(delayMs, cfg.priority)
	res, err := q.client.scripts.Run(ctx, scriptName, ks, argsBytes, dataJSON, optsBytes)
	if err != nil {
		return nil, fmt.Errorf("mkq: %s: %w", scriptName, err)
	}

	jobID, err := parseAddResult(res)
	if err != nil {
		return nil, err
	}
	span.SetAttrs(slog.String(AttrJobID, jobID))

	job = &Job[T]{
		ID:        jobID,
		Name:      jobName,
		Data:      payload,
		Timestamp: time.UnixMilli(timestampMs),
		queue:     q,
	}
	if preDedupID != "" && preDedupID == jobID {
		// dedup hit — 既存 job をそのまま返しつつ ErrDuplicateJob で
		// 通知。Job.Data は呼び出し側 payload なので、本当に既存 job
		// の payload が欲しい場合は Queue.Get(jobID) で再取得する。
		// dedup hit は新規 add ではないので jobs_added_total はインクリ
		// しない。span 上は error 扱いせず attribute で識別だけ残す
		// (defer の SetError ガードと連動)。
		span.SetAttrs(slog.Bool("mkq.dedup.hit", true))
		err = ErrDuplicateJob
		return job, err
	}
	q.client.metrics.CounterAdd(MetricJobsAddedTotal, 1,
		slog.String(AttrQueue, q.name),
		slog.String(AttrJobName, jobName),
	)
	return job, nil
}

// dispatch picks the BullMQ Lua entry point and assembles its KEYS list
// per the directives at the top of each .lua file. The slice ordering
// is load-bearing — Lua references KEYS[N] positionally.
//
// delayMs / priority are passed in already-coerced (vs raw cfg) so the
// rounding rules around sub-ms WithDelay live in one place: the caller.
func (q *Queue[T]) dispatch(delayMs int64, priority uint32) (lua.ScriptName, []string) {
	switch {
	case delayMs > 0:
		// addDelayedJob-6 KEYS:
		//   1 marker, 2 meta, 3 id, 4 delayed, 5 completed, 6 events
		return lua.AddDelayedJob, []string{
			q.keys.Marker(),
			q.keys.Meta(),
			q.keys.ID(),
			q.keys.Delayed(),
			q.keys.Completed(),
			q.keys.Events(),
		}
	case priority > 0:
		// addPrioritizedJob-9 KEYS:
		//   1 marker, 2 meta, 3 id, 4 prioritized, 5 delayed,
		//   6 completed, 7 active, 8 events, 9 pc
		return lua.AddPrioritizedJob, []string{
			q.keys.Marker(),
			q.keys.Meta(),
			q.keys.ID(),
			q.keys.Prioritized(),
			q.keys.Delayed(),
			q.keys.Completed(),
			q.keys.Active(),
			q.keys.Events(),
			q.keys.PriorityCounter(),
		}
	default:
		// addStandardJob-9 KEYS:
		//   1 wait, 2 paused, 3 meta, 4 id, 5 completed,
		//   6 delayed, 7 active, 8 events, 9 marker
		return lua.AddStandardJob, []string{
			q.keys.Wait(),
			q.keys.Paused(),
			q.keys.Meta(),
			q.keys.ID(),
			q.keys.Completed(),
			q.keys.Delayed(),
			q.keys.Active(),
			q.keys.Events(),
			q.keys.Marker(),
		}
	}
}

// toProtoOpts adapts the public addConfig to the wire-level proto.AddOpts.
//
// delayMs is the already-coerced millisecond offset (matching BullMQ's
// opts.delay semantics): the vendored Lua adds it to args[4] timestamp
// to derive the absolute delayed target.
func toProtoOpts(cfg addConfig, delayMs int64) proto.AddOpts {
	o := proto.AddOpts{
		Delay:            delayMs,
		Priority:         cfg.priority,
		Attempts:         cfg.attempts,
		Lifo:             cfg.lifo,
		RemoveOnComplete: buildRetentionLimit(cfg.keepCompleted, cfg.keepCompletedAge),
		RemoveOnFail:     buildRetentionLimit(cfg.keepFailed, cfg.keepFailedAge),
	}
	if cfg.backoff != nil {
		o.Backoff = &proto.Backoff{
			Type:   cfg.backoff.Type,
			Delay:  cfg.backoff.Delay.Milliseconds(),
			Jitter: cfg.backoff.Jitter,
		}
	}
	if cfg.dedupID != "" {
		o.Deduplication = &proto.Deduplication{
			ID:        cfg.dedupID,
			TTLMillis: cfg.dedupTTL.Milliseconds(),
		}
	}
	return o
}

// buildRetentionLimit folds the per-Add WithKeep* options into the
// proto wire shape. nil result lets the encoder skip the key entirely.
func buildRetentionLimit(count *int, age *time.Duration) *proto.RetentionLimit {
	if count == nil && age == nil {
		return nil
	}
	rl := &proto.RetentionLimit{Count: count}
	if age != nil {
		// 1-second resolution matches BullMQ. Sub-second values
		// truncate; negatives are passed through to mirror BullMQ's
		// "no validation" stance for `removeOnComplete: { age: -1 }`.
		secs := int(age.Seconds())
		rl.AgeSeconds = &secs
	}
	return rl
}

// Get returns a snapshot of the BullMQ HASH for jobID. Useful for
// admin / inspector flows; the worker dispatch path does not call
// this. Returns ErrJobNotFound when no HASH exists at the expected
// key.
//
// The Job[T] return value carries the user-facing identity (ID /
// Name / Data) and the dequeue counters (AttemptsMade etc.). The
// JobState carries fields that only become meaningful after
// processing starts (ProcessedOn / FinishedOn / ReturnValue / ...).
func (q *Queue[T]) Get(ctx context.Context, jobID string) (*Job[T], *JobState, error) {
	hash, err := q.client.rdb.HGetAll(ctx, q.keys.Job(jobID)).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("mkq: HGetAll %s: %w", jobID, err)
	}
	if len(hash) == 0 {
		return nil, nil, ErrJobNotFound
	}
	job, err := buildJob[T](jobID, hash)
	if err != nil {
		return nil, nil, err
	}
	job.queue = q
	return job, buildJobState(hash), nil
}

// buildJobState extracts the post-processing fields from a HGETALL
// snapshot. Missing fields stay at their Go zero values.
//
// Timestamp fields are parsed via strconv.ParseInt with bitSize=64
// rather than parseInt (which delegates to strconv.Atoi → int) so
// that 13-digit Unix-millisecond values survive on 32-bit platforms
// where int wraps at ~2.1 × 10^9. Mirrors the existing buildJob
// timestamp parsing in worker.go.
func buildJobState(hash map[string]string) *JobState {
	st := &JobState{
		FailedReason: hash["failedReason"],
	}
	if v, err := strconv.ParseInt(hash["processedOn"], 10, 64); err == nil && v > 0 {
		st.ProcessedOn = time.UnixMilli(v)
	}
	if v, err := strconv.ParseInt(hash["finishedOn"], 10, 64); err == nil && v > 0 {
		st.FinishedOn = time.UnixMilli(v)
	}
	if rv := hash["returnvalue"]; rv != "" {
		st.ReturnValue = json.RawMessage(rv)
	}
	if pg := hash["progress"]; pg != "" {
		st.Progress = json.RawMessage(pg)
	}
	if o := hash["opts"]; o != "" {
		st.Opts = json.RawMessage(o)
	}
	if v, err := strconv.ParseInt(hash["delay"], 10, 64); err == nil {
		st.Delay = v
	}
	st.AttemptsAt = decodeJSONInt64s(hash[attemptHistoryField])
	if st_ := hash["stacktrace"]; st_ != "" {
		// BullMQ は stacktrace を JSON エンコードされた文字列配列として保存する。
		// best-effort デコード; 不正入力 → 空スライス。
		var traces []string
		if err := json.Unmarshal([]byte(st_), &traces); err == nil {
			st.Stacktrace = traces
		}
	}
	return st
}

// parseAddResult coerces the polymorphic EVALSHA return into a job id
// string. BullMQ Lua returns the id as a string by appending `""`.
// Negative integer returns are reserved for error codes (e.g. -5 for
// "missing parent key").
func parseAddResult(res any) (string, error) {
	switch v := res.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int64:
		if v < 0 {
			return "", fmt.Errorf("mkq: lua add returned error code %d", v)
		}
		return strconv.FormatInt(v, 10), nil
	default:
		return "", fmt.Errorf("mkq: unexpected lua add result %T: %v", res, res)
	}
}
