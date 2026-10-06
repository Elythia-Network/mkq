package mkq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/elythia-network/mkq/internal/keys"
)

// Event is the marker interface for entries surfaced by QueueEvents.
// All concrete event types embed an ID() method returning the
// Redis Stream entry ID (e.g. "1700000000000-0") so callers can
// resume from a specific offset later if a history-replay API
// lands.
type Event interface {
	// ID returns the Redis Stream entry id. Stable across redeliveries
	// (each entry keeps its id even if the underlying stream is
	// trimmed).
	ID() string
	isQueueEvent()
}

// AddedEvent corresponds to BullMQ's `added` event (storeJob.lua).
type AddedEvent struct {
	id    string
	JobID string
	Name  string
}

// WaitingEvent corresponds to BullMQ's `waiting` event. Prev describes
// the previous state if any (e.g. "active" when retryJob promotes a
// failed attempt back to wait).
type WaitingEvent struct {
	id    string
	JobID string
	Prev  string
}

// ActiveEvent corresponds to BullMQ's `active` event
// (prepareJobForProcessing.lua).
type ActiveEvent struct {
	id    string
	JobID string
	Prev  string
}

// CompletedEvent corresponds to BullMQ's `completed` event
// (moveToFinished.lua, target=completed). ReturnValue carries the
// JSON-stringified handler return verbatim.
type CompletedEvent struct {
	id          string
	JobID       string
	ReturnValue json.RawMessage
	Prev        string
}

// FailedEvent corresponds to BullMQ's `failed` event
// (moveToFinished.lua, target=failed). FailedReason is the plain-text
// error message (no JSON encoding) per BullMQ TS convention.
type FailedEvent struct {
	id           string
	JobID        string
	FailedReason string
	Prev         string
}

// DelayedEvent corresponds to BullMQ's `delayed` event (addDelayedJob,
// moveToDelayed). DelayedUntilMs is the absolute target timestamp
// (ms since unix epoch) at which the job becomes eligible to run —
// not a relative duration. The Lua-emitted `delay` field carries
// this absolute value, hence the renamed Go field.
type DelayedEvent struct {
	id             string
	JobID          string
	DelayedUntilMs int64
}

// StalledEvent corresponds to BullMQ's `stalled` event
// (moveStalledJobsToWait.lua).
type StalledEvent struct {
	id    string
	JobID string
}

// DrainedEvent corresponds to BullMQ's `drained` event, fired when
// the last active job finalises and no more wait/prioritized/delayed
// work is queued.
type DrainedEvent struct {
	id string
}

// RetriesExhaustedEvent corresponds to BullMQ's `retries-exhausted`
// event, fired when a failed job's atm reaches the configured
// attempts cap (gated by moveToFinished's `attemptsMade >= attempts`
// check).
type RetriesExhaustedEvent struct {
	id           string
	JobID        string
	AttemptsMade int
}

// RemovedEvent corresponds to BullMQ's `removed` event (deduplicateJob
// and similar paths that drop a job key). Prev describes the state
// the job left when removed (e.g. "delayed").
type RemovedEvent struct {
	id    string
	JobID string
	Prev  string
}

// ProgressEvent corresponds to BullMQ's `progress` event, emitted by
// updateProgress-3.lua whenever a worker (or out-of-band caller)
// reports a progress value via Job.UpdateProgress /
// Queue.UpdateJobProgress. Data is the JSON-encoded progress value
// — number, string, object — exactly as the lua wrote it to the
// stream's `data` field.
type ProgressEvent struct {
	id    string
	JobID string
	Data  json.RawMessage
}

// RawEvent wraps any event type mkq doesn't model explicitly. Future
// BullMQ versions can emit new event names (or mutation features land
// in #27), and this type keeps Subscribe forward-compatible.
type RawEvent struct {
	id     string
	Type   string
	Fields map[string]string
}

// ID accessors. Each event embeds the stream entry id privately so
// callers can read it via the interface method without colliding with
// JobID/Name fields.
func (e AddedEvent) ID() string            { return e.id }
func (e WaitingEvent) ID() string          { return e.id }
func (e ActiveEvent) ID() string           { return e.id }
func (e CompletedEvent) ID() string        { return e.id }
func (e FailedEvent) ID() string           { return e.id }
func (e DelayedEvent) ID() string          { return e.id }
func (e StalledEvent) ID() string          { return e.id }
func (e DrainedEvent) ID() string          { return e.id }
func (e RetriesExhaustedEvent) ID() string { return e.id }
func (e RemovedEvent) ID() string          { return e.id }
func (e ProgressEvent) ID() string         { return e.id }
func (e RawEvent) ID() string              { return e.id }

func (AddedEvent) isQueueEvent()            {}
func (WaitingEvent) isQueueEvent()          {}
func (ActiveEvent) isQueueEvent()           {}
func (CompletedEvent) isQueueEvent()        {}
func (FailedEvent) isQueueEvent()           {}
func (DelayedEvent) isQueueEvent()          {}
func (StalledEvent) isQueueEvent()          {}
func (DrainedEvent) isQueueEvent()          {}
func (RetriesExhaustedEvent) isQueueEvent() {}
func (RemovedEvent) isQueueEvent()          {}
func (ProgressEvent) isQueueEvent()         {}
func (RawEvent) isQueueEvent()              {}

// QueueEvents is a long-lived subscriber to a queue's BullMQ events
// stream. It does not maintain background goroutines until Subscribe
// is called.
type QueueEvents struct {
	keys keys.Builder
	rdb  redis.UniversalClient

	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewQueueEvents creates a subscriber for q's events stream. Construct
// one per logical subscription; the underlying Redis client is shared
// with the queue's Client.
func NewQueueEvents[T any](q *Queue[T]) *QueueEvents {
	return &QueueEvents{
		keys:    q.keys,
		rdb:     q.client.rdb,
		closeCh: make(chan struct{}),
	}
}

// Close terminates an in-flight Subscribe. Safe to call multiple times.
// After Close, Subscribe returns nil on the next loop iteration.
//
// A QueueEvents instance is single-use: once Close has been called the
// underlying channel stays closed forever, so any subsequent Subscribe
// call returns nil immediately without delivering events. Construct
// a fresh QueueEvents (NewQueueEvents) when you need a new
// subscription.
func (qe *QueueEvents) Close() {
	qe.closeOnce.Do(func() { close(qe.closeCh) })
}

// streamBlockTimeout caps each XREAD BLOCK call. We pick a short value
// so ctx cancellation surfaces within ~one tick rather than waiting on
// the BullMQ default (5s+) — at this granularity Redis sees one round
// trip per ~200ms idle, which is negligible.
const streamBlockTimeout = 200 * time.Millisecond

// Subscribe consumes the events stream and dispatches each entry to
// handler. It returns when:
//   - ctx is cancelled (returns ctx.Err()).
//   - Close is called (returns nil).
//   - handler returns a non-nil error (returns that error).
//
// The subscriber starts at "$" — only events written after Subscribe
// is called are delivered. Past entries are ignored.
func (qe *QueueEvents) Subscribe(ctx context.Context, handler func(Event) error) error {
	if handler == nil {
		return errors.New("mkq: QueueEvents.Subscribe requires a non-nil handler")
	}

	streamKey := qe.keys.Events()
	lastID := "$"

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-qe.closeCh:
			return nil
		default:
		}

		// XREAD BLOCK with a short timeout so we can re-check
		// ctx/closeCh each tick. go-redis surfaces a `redis.Nil`
		// (no entries) on timeout; treat it as "loop again".
		streams, err := qe.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Block:   streamBlockTimeout,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return fmt.Errorf("mkq: XREAD events: %w", err)
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				ev := decodeStreamMessage(msg)
				lastID = msg.ID
				if err := handler(ev); err != nil {
					return err
				}
			}
		}
	}
}

// decodeStreamMessage converts a go-redis XMessage into a typed Event.
// The BullMQ wire format keys every entry with an `event` field; the
// rest of the fields are payload specific to each type.
func decodeStreamMessage(msg redis.XMessage) Event {
	fields := make(map[string]string, len(msg.Values))
	for k, v := range msg.Values {
		switch x := v.(type) {
		case string:
			fields[k] = x
		case []byte:
			fields[k] = string(x)
		default:
			// Defensive: go-redis surfaces strings here in practice,
			// but keep RawEvent functional for surprise types.
			fields[k] = fmt.Sprintf("%v", v)
		}
	}

	switch fields["event"] {
	case "added":
		return AddedEvent{id: msg.ID, JobID: fields["jobId"], Name: fields["name"]}
	case "waiting":
		return WaitingEvent{id: msg.ID, JobID: fields["jobId"], Prev: fields["prev"]}
	case "active":
		return ActiveEvent{id: msg.ID, JobID: fields["jobId"], Prev: fields["prev"]}
	case "completed":
		return CompletedEvent{
			id:          msg.ID,
			JobID:       fields["jobId"],
			ReturnValue: rawJSON(fields["returnvalue"]),
			Prev:        fields["prev"],
		}
	case "failed":
		return FailedEvent{
			id:           msg.ID,
			JobID:        fields["jobId"],
			FailedReason: fields["failedReason"],
			Prev:         fields["prev"],
		}
	case "delayed":
		delay, _ := strconv.ParseInt(fields["delay"], 10, 64)
		return DelayedEvent{id: msg.ID, JobID: fields["jobId"], DelayedUntilMs: delay}
	case "stalled":
		return StalledEvent{id: msg.ID, JobID: fields["jobId"]}
	case "drained":
		return DrainedEvent{id: msg.ID}
	case "retries-exhausted":
		atm, _ := strconv.Atoi(fields["attemptsMade"])
		return RetriesExhaustedEvent{id: msg.ID, JobID: fields["jobId"], AttemptsMade: atm}
	case "removed":
		return RemovedEvent{id: msg.ID, JobID: fields["jobId"], Prev: fields["prev"]}
	case "progress":
		return ProgressEvent{
			id:    msg.ID,
			JobID: fields["jobId"],
			Data:  rawJSON(fields["data"]),
		}
	default:
		// 未知 event は forward compat のため RawEvent で wrap。
		return RawEvent{id: msg.ID, Type: fields["event"], Fields: fields}
	}
}

func rawJSON(s string) json.RawMessage {
	if s == "" {
		return nil
	}
	return json.RawMessage(s)
}
