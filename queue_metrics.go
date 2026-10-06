package mkq

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/elythia-network/mkq/internal/lua"
)

// QueueMetrics is the BullMQ-compatible per-minute job count snapshot
// returned by Queue.GetMetrics. Layout mirrors BullMQ TS's
// `Queue.getMetrics(...)` result one-for-one so any client already
// reading mkq queues via that API sees identical fields.
type QueueMetrics struct {
	// Meta is the live metadata HASH backing the metric. PrevTS /
	// PrevCount drive the next bucket's delta; Count is the
	// cumulative job count since the worker enabled WithJobMetrics.
	Meta QueueMetricsMeta
	// Data carries the per-minute deltas, newest first (LRANGE
	// order). Each entry is the count of jobs finalised during that
	// minute. Zeros pad gaps when a minute had no traffic and a
	// later minute saw activity.
	Data []int64
	// Count is the total number of points stored in the data LIST
	// (LLEN), independent of the start/end window the caller
	// requested. Useful for paginating large histories.
	Count int64
}

// QueueMetricsMeta is the HASH-backed live counter metadata stored
// at `bull:<queue>:metrics:<target>`.
type QueueMetricsMeta struct {
	Count     int64 // cumulative since metrics enabled
	PrevTS    int64 // ms epoch of the last bucket roll
	PrevCount int64 // cumulative count at PrevTS (delta source)
}

// ErrInvalidMetricsBucket is returned by GetMetrics when kind is not
// one of JobBucketCompleted / JobBucketFailed. BullMQ only collects
// those two finalisation kinds; other buckets have no metrics
// counterpart in the Lua scripts.
var ErrInvalidMetricsBucket = errors.New("mkq: GetMetrics kind must be JobBucketCompleted or JobBucketFailed")

// GetMetrics reads the per-minute completed / failed metrics
// populated by a worker that ran with WithJobMetrics. Layout matches
// BullMQ TS's Queue.getMetrics:
//
//   - kind selects completed vs failed.
//   - start / end follow LRANGE semantics into the data LIST: 0 is
//     the newest entry, -1 is the oldest. (start=0, end=-1) reads
//     every stored point.
//
// Returns a zero-valued QueueMetrics (with Count=0) if the worker
// has never enabled metrics for this queue — the metrics keys
// simply don't exist, so HMGET returns three nils and LRANGE
// returns an empty slice.
//
// One Redis round-trip via the vendored getMetrics-2.lua, so the
// HMGET + LRANGE + LLEN observe a consistent snapshot.
func (q *Queue[T]) GetMetrics(ctx context.Context, kind JobBucket, start, end int64) (QueueMetrics, error) {
	if kind != JobBucketCompleted && kind != JobBucketFailed {
		return QueueMetrics{}, fmt.Errorf("%w (got %q)", ErrInvalidMetricsBucket, kind)
	}
	metricsKey := q.keys.Metrics(string(kind))
	dataKey := metricsKey + ":data"

	res, err := q.client.scripts.Run(
		ctx,
		lua.GetMetrics,
		[]string{metricsKey, dataKey},
		start, end,
	)
	if err != nil {
		return QueueMetrics{}, fmt.Errorf("mkq: getMetrics: %w", err)
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return QueueMetrics{}, fmt.Errorf("mkq: getMetrics: unexpected result type %T", res)
	}

	meta, err := parseMetricsMeta(arr[0])
	if err != nil {
		return QueueMetrics{}, err
	}
	data, err := parseMetricsData(arr[1])
	if err != nil {
		return QueueMetrics{}, err
	}
	count, _ := toInt64(arr[2])

	return QueueMetrics{
		Meta:  meta,
		Data:  data,
		Count: count,
	}, nil
}

// parseMetricsMeta extracts {count, prevTS, prevCount} from the
// HMGET result. BullMQ Lua emits each field as a Redis bulk string
// (or nil if the key is absent) — go-redis surfaces nil as a Go nil
// inside []any, which we treat as the zero value.
func parseMetricsMeta(raw any) (QueueMetricsMeta, error) {
	arr, ok := raw.([]any)
	if !ok {
		return QueueMetricsMeta{}, fmt.Errorf("mkq: getMetrics meta: unexpected type %T", raw)
	}
	if len(arr) != 3 {
		return QueueMetricsMeta{}, fmt.Errorf("mkq: getMetrics meta: expected 3 fields, got %d", len(arr))
	}
	return QueueMetricsMeta{
		Count:     parseMetricsInt(arr[0]),
		PrevTS:    parseMetricsInt(arr[1]),
		PrevCount: parseMetricsInt(arr[2]),
	}, nil
}

// parseMetricsData converts the LRANGE result (each element a Redis
// bulk string) into typed deltas.
func parseMetricsData(raw any) ([]int64, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("mkq: getMetrics data: unexpected type %T", raw)
	}
	out := make([]int64, len(arr))
	for i, v := range arr {
		out[i] = parseMetricsInt(v)
	}
	return out, nil
}

// parseMetricsInt reads a Redis bulk string into int64. nil / empty
// values become 0; parse errors also become 0 (matching BullMQ TS's
// `+point || 0` fallback in queue-getters.ts:646).
func parseMetricsInt(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		if x == "" {
			return 0
		}
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case int64:
		return x
	default:
		if n, ok := toInt64(v); ok {
			return n
		}
		return 0
	}
}
