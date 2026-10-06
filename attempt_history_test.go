package mkq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// 失敗のたびに stacktrace を積み、その試行の開始時刻を残すこと。
//
// **上書きではなく append。** BullMQ TS は stacktrace を試行ごとに積む。mkq は
// 1 要素で上書きしていたので、再試行した job の**過去の失敗理由が残らなかった**。
// あわせて試行の開始時刻を記録する (BullMQ には対応物が無い。admin UI が
// 再試行を時系列に並べられるようにするため)。
func TestFailureHistory_AccumulatesAcrossRetries(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{}, mkq.WithAttempts(3))
	require.NoError(t, err)

	var tries int
	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		tries++
		return nil, errors.New("boom")
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"failed", added.ID).Result()
		return v > 0
	})

	_, st, err := queue.Get(ctx, added.ID)
	require.NoError(t, err)
	require.NotNil(t, st)

	// 3 回とも理由が残ること (以前は最後の 1 件だけだった)。
	assert.Len(t, st.Stacktrace, 3, "試行ごとに積むこと")
	for _, s := range st.Stacktrace {
		assert.Equal(t, "boom", s)
	}

	// 試行の開始時刻が 3 つ、昇順で残ること。
	require.Len(t, st.AttemptsAt, 3, "試行ごとの開始時刻を残すこと")
	for i := 1; i < len(st.AttemptsAt); i++ {
		assert.GreaterOrEqual(t, st.AttemptsAt[i], st.AttemptsAt[i-1], "昇順であること")
	}
	// 最後の試行の開始時刻は processedOn と一致する。
	assert.Equal(t, st.ProcessedOn.UnixMilli(), st.AttemptsAt[len(st.AttemptsAt)-1])
	assert.Equal(t, 3, tries)
}

// 成功した job には履歴が付かないこと。
func TestFailureHistory_AbsentOnSuccess(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	worker, err := mkq.Process(queue, func(context.Context, *mkq.Job[testPayload]) (any, error) {
		return nil, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Stop(context.Background()) })

	rdb := rawClient(t)
	base := prefix + ":deliver:"
	waitFor(t, ctx, 20*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", added.ID).Result()
		return v > 0
	})

	_, st, err := queue.Get(ctx, added.ID)
	require.NoError(t, err)
	assert.Empty(t, st.Stacktrace)
	assert.Empty(t, st.AttemptsAt)
}
