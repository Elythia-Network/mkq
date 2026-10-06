package mkq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// admin UI の job log タブ用に、書いた log を読み戻せること。
// BullMQ の Queue.getJobLogs と同じく (logs, count) を返す。
func TestGetJobLogs_ReturnsAppendedLines(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)
	require.NoError(t, queue.AppendJobLog(ctx, added.ID, "first"))
	require.NoError(t, queue.AppendJobLog(ctx, added.ID, "second"))
	require.NoError(t, queue.AppendJobLog(ctx, added.ID, "third"))

	all, err := queue.GetJobLogs(ctx, added.ID, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second", "third"}, all.Logs)
	assert.EqualValues(t, 3, all.Count)

	// 範囲指定しても count は全体の長さを返す (UI が "N 件中" を出せる)。
	page, err := queue.GetJobLogs(ctx, added.ID, 0, 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second"}, page.Logs)
	assert.EqualValues(t, 3, page.Count, "count は範囲でなく全体の長さ")
}

// log の無い job と存在しない job はどちらも空。BullMQ の getJobLogs も
// `<job>:logs` を読むだけで job HASH の存在を見ないので、区別しない。
func TestGetJobLogs_EmptyForJobWithoutLogs(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	got, err := queue.GetJobLogs(ctx, added.ID, 0, -1)
	require.NoError(t, err)
	assert.Empty(t, got.Logs)
	assert.Zero(t, got.Count)
	// **nil ではなく空スライスを返す。** 呼び出し側が JSON に出すとき
	// nil だと null になり、配列を期待する UI が壊れる。
	assert.NotNil(t, got.Logs)

	missing, err := queue.GetJobLogs(ctx, "does-not-exist", 0, -1)
	require.NoError(t, err)
	assert.Empty(t, missing.Logs)
	assert.NotNil(t, missing.Logs)
}

func TestGetJobLogs_RejectsEmptyJobID(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.GetJobLogs(ctx, "", 0, -1)
	require.Error(t, err)
}
