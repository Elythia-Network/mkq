//go:build interop

// Retention parity for job schedulers.
//
// BullMQ keeps a scheduler's per-iteration job options on the scheduler
// HASH under `opts`, and its Worker reads that back when it queues the
// next iteration. If mkq wrote a different shape there, a BullMQ TS
// worker sharing the queue would either drop the retention or choke on
// it — and the jobs mkq's own scheduler creates would stop being
// readable as BullMQ scheduled jobs.

package interop_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// nodeUpsertScheduler drives BullMQ TS's own upsertJobScheduler.
func nodeUpsertScheduler(t *testing.T, prefix, queueName, scheduleID string, everyMs int, templateOptsJSON string) string {
	t.Helper()
	var got struct {
		JobID string `json:"jobId"`
	}
	runNodeScript(t, "scheduler.js", prefix, queueName, &got,
		"upsert", scheduleID, strconv.Itoa(everyMs), templateOptsJSON)
	require.NotEmpty(t, got.JobID, "BullMQ TS must have queued the first iteration")
	return got.JobID
}

// TestInterop_ScheduleRetention_TemplateMatchesBullMQ pins mkq's
// scheduler template against the one BullMQ TS writes for the same
// retention, field for field.
func TestInterop_ScheduleRetention_TemplateMatchesBullMQ(t *testing.T) {
	const queueName = "tick"
	const scheduleID = "ticker"
	const everyMs = 3600000

	// BullMQ TS 側
	tsPrefix := uniquePrefix(t)
	_ = nodeUpsertScheduler(t, tsPrefix, queueName, scheduleID, everyMs,
		`{"removeOnComplete":{"age":604800},"removeOnFail":50}`)

	// mkq 側
	mkqPrefix := uniquePrefix(t)
	c := newClient(t, mkqPrefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, scheduleID,
		time.Duration(everyMs)*time.Millisecond, interopPayload{},
		mkq.WithScheduleKeepCompletedAge(7*24*time.Hour),
		mkq.WithScheduleKeepFailed(50),
	))

	rdb := rawClient(t)
	readTemplate := func(prefix string) map[string]any {
		t.Helper()
		raw, err := rdb.HGet(ctx, prefix+":"+queueName+":repeat:"+scheduleID, "opts").Result()
		require.NoError(t, err, "scheduler HASH must carry an opts template")
		var out map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &out))
		return out
	}

	ts := readTemplate(tsPrefix)
	got := readTemplate(mkqPrefix)

	assert.Equal(t, ts["removeOnComplete"], got["removeOnComplete"],
		"removeOnComplete must be written in BullMQ's shape")
	assert.Equal(t, ts["removeOnFail"], got["removeOnFail"],
		"removeOnFail must be written in BullMQ's shape")

	// 参照側が本当に retention を書いていることも確かめる。両方が空なら
	// 上の比較は何も証明しない。
	require.Contains(t, ts, "removeOnComplete",
		"BullMQ TS itself must persist the template retention; otherwise the parity check proves nothing")
	assert.EqualValues(t, 604800, ts["removeOnComplete"].(map[string]any)["age"])
}

// The first iteration's job HASH has to carry the same retention, since
// that is what moveToFinished reads when the job completes.
func TestInterop_ScheduleRetention_IterationOptsMatchBullMQ(t *testing.T) {
	const queueName = "tick"
	const scheduleID = "ticker"
	const everyMs = 3600000
	const template = `{"removeOnComplete":{"age":604800}}`

	tsPrefix := uniquePrefix(t)
	_ = nodeUpsertScheduler(t, tsPrefix, queueName, scheduleID, everyMs, template)

	mkqPrefix := uniquePrefix(t)
	c := newClient(t, mkqPrefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, queue.UpsertScheduleEvery(ctx, scheduleID,
		time.Duration(everyMs)*time.Millisecond, interopPayload{},
		mkq.WithScheduleKeepCompletedAge(7*24*time.Hour),
	))

	rdb := rawClient(t)
	firstIterationOpts := func(prefix string) map[string]any {
		t.Helper()
		ids, err := rdb.LRange(ctx, prefix+":"+queueName+":wait", 0, -1).Result()
		require.NoError(t, err)
		require.Len(t, ids, 1, "one iteration must be queued")
		raw, err := rdb.HGet(ctx, prefix+":"+queueName+":"+ids[0], "opts").Result()
		require.NoError(t, err)
		var out map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &out))
		return out
	}

	ts := firstIterationOpts(tsPrefix)
	got := firstIterationOpts(mkqPrefix)

	assert.Equal(t, ts["removeOnComplete"], got["removeOnComplete"],
		"the iteration's own opts must carry the retention in BullMQ's shape")
	require.Contains(t, ts, "removeOnComplete",
		"BullMQ TS itself must put the retention on the iteration")

	// repeat ブロックが retention と共存していること。片方が他方を
	// 上書きしていたら foreign worker が再スケジュールできない。
	require.Contains(t, got, "repeat")
	require.Contains(t, ts, "repeat")
}

// TestInterop_ScheduleTemplate_SurvivesMkqReschedule pins the whole
// template, not just the retention.
//
// **mkq の worker が 2 本目を積むとき、template は丸ごと持ち越される。**
// 知っているフィールドだけを写すと、BullMQ TS が `attempts: 3` 付きで作った
// schedule を mkq が回した途端に再試行しなくなり、`priority` に至っては
// 置き場所 (prioritized ZSET か wait か) まで変わる。
func TestInterop_ScheduleTemplate_SurvivesMkqReschedule(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "tick"
	const scheduleID = "carry"

	// BullMQ TS 側が template を書く。mkq には対応する ScheduleOption が
	// 無いフィールドばかりを選んである。
	firstID := nodeUpsertScheduler(t, prefix, queueName, scheduleID, 200,
		`{"attempts":3,"backoff":{"type":"exponential","delay":1000},"priority":5}`)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rdb := rawClient(t)

	// mkq の worker に 1 本処理させ、次の iteration を積ませる。
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[interopPayload]) (any, error) {
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer stopWorker(t, worker)

	var nextID string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && nextID == "" {
		for _, id := range zrangeOrEmpty(t, ctx, rdb, prefix+":"+queueName+":delayed") {
			if strings.HasPrefix(id, "repeat:"+scheduleID+":") && id != firstID {
				nextID = id
			}
		}
		if nextID == "" {
			time.Sleep(50 * time.Millisecond)
		}
	}
	require.NotEmpty(t, nextID, "mkq の worker が次の iteration を積まなかった")

	raw, err := rdb.HGet(ctx, prefix+":"+queueName+":"+nextID, "opts").Result()
	require.NoError(t, err)
	var opts map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &opts))

	assert.EqualValues(t, 3, opts["attempts"], "attempts が落ちると 2 本目から再試行しなくなる")
	assert.EqualValues(t, 5, opts["priority"], "priority が落ちると置き場所が変わる")
	assert.Equal(t, map[string]any{"type": "exponential", "delay": float64(1000)}, opts["backoff"])

	// **整数が整数のままかは、ここでは確かめられない。** Go 側が float64 を
	// 送っても Redis の cjson が整数値の double を `3` として書き戻すので、
	// wire を見ても区別が付かない。Go 側の型は
	// TestDecodeScheduleTemplateOpts_KeepsIntegersIntegral で固定してある。
	// ここに `NotContains(raw, "3.0")` を足しても落ちない (確認済み)。

	// scheduler 由来であることの印が残っていること。これが落ちると stalled
	// 回収時に周期ジョブと判定されず、恒久 fail する (moveStalledJobsToWait-9)。
	rjk, err := rdb.HGet(ctx, prefix+":"+queueName+":"+nextID, "rjk").Result()
	require.NoError(t, err)
	assert.Equal(t, scheduleID, rjk)
}

func zrangeOrEmpty(t *testing.T, ctx context.Context, rdb interface {
	ZRange(context.Context, string, int64, int64) *redis.StringSliceCmd
}, key string) []string {
	t.Helper()
	ids, err := rdb.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil
	}
	return ids
}
