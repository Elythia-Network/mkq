package mkq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// JobState は job HASH のスナップショットなので、opts / delay も
// そのまま読めること。admin UI が BullMQ の Job.opts を丸ごと表示する
// 用途で要る (加工すると知らない key が黙って消える)。
func TestJobState_CarriesOptsAndDelay(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{Inbox: "https://remote.example/inbox"},
		mkq.WithAttempts(7), mkq.WithDelay(3*time.Second))
	require.NoError(t, err)

	_, st, err := queue.Get(ctx, added.ID)
	require.NoError(t, err)
	require.NotNil(t, st)

	// delay はミリ秒。
	assert.Equal(t, int64(3000), st.Delay)

	// opts は生の JSON。attempts が読めることを確認する (UI が
	// `job.opts.attempts` を直接参照する)。
	require.NotEmpty(t, st.Opts, "opts が空だと admin UI の Options タブが空になる")
	var opts map[string]any
	require.NoError(t, json.Unmarshal(st.Opts, &opts))
	assert.EqualValues(t, 7, opts["attempts"])
}

// delay 無しで Add した job は Delay=0。opts は必ず入る。
func TestJobState_OptsPresentWithoutDelay(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	added, err := queue.Add(ctx, testPayload{})
	require.NoError(t, err)

	_, st, err := queue.Get(ctx, added.ID)
	require.NoError(t, err)
	assert.Zero(t, st.Delay)
	assert.NotEmpty(t, st.Opts)
}

// ListJobs 経由の JobState も同じ内容を持つこと。admin の一覧と詳細で
// 出せる情報が食い違わないようにする。
func TestJobState_ListJobsCarriesOpts(t *testing.T) {
	t.Parallel()
	prefix := uniquePrefix(t)
	c := newClient(t, prefix)
	queue := mkq.Define[testPayload](c, "deliver")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := queue.Add(ctx, testPayload{}, mkq.WithAttempts(4))
	require.NoError(t, err)

	listed, err := queue.ListJobs(ctx, mkq.JobBucketWait, 0, -1, true)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.NotNil(t, listed[0].State)
	require.NotEmpty(t, listed[0].State.Opts)
	var opts map[string]any
	require.NoError(t, json.Unmarshal(listed[0].State.Opts, &opts))
	assert.EqualValues(t, 4, opts["attempts"])
}
