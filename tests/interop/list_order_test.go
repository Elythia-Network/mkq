//go:build interop

// Ordering parity for Queue.getJobs / ListJobs.
//
// BullMQ TS and mkq run the same `getRanges-1.lua`, which for LIST
// buckets picks the right window but leaves it in LIST-native order.
// BullMQ reverses that client-side when asc is set
// (`src/classes/queue-getters.ts`); mkq has to do the same or the two
// disagree on an API both claim to implement. mkq did not, which is
// what this file guards against coming back.

package interop_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// nodeGetJobs asks BullMQ TS for the ids in a bucket over [start, end].
func nodeGetJobs(t *testing.T, prefix, queueName, state string, start, end int, asc bool) []string {
	t.Helper()
	var got struct {
		IDs []string `json:"ids"`
	}
	ascArg := "false"
	if asc {
		ascArg = "true"
	}
	runNodeInspector(t, prefix, queueName, &got, "getJobs", state,
		strconv.Itoa(start), strconv.Itoa(end), ascArg)
	return got.IDs
}

func mkqListIDs(t *testing.T, ctx context.Context, q *mkq.Queue[interopPayload], b mkq.JobBucket, start, end int64, asc bool) []string {
	t.Helper()
	listed, err := q.ListJobs(ctx, b, start, end, asc)
	require.NoError(t, err)
	ids := make([]string, 0, len(listed))
	for _, lj := range listed {
		ids = append(ids, lj.Job.ID)
	}
	return ids
}

// TestInterop_ListOrder_WaitMatchesBullMQ is the parity assertion that
// would have caught the bug: mkq and BullMQ TS must return the same
// ids in the same order for the same (bucket, start, end, asc).
func TestInterop_ListOrder_WaitMatchesBullMQ(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "listorder"
	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	added := make([]string, 0, 5)
	for i := range 5 {
		job, err := queue.Add(ctx, interopPayload{Inbox: "x", Body: string(rune('a' + i))})
		require.NoError(t, err)
		added = append(added, job.ID)
	}

	for _, tc := range []struct {
		name       string
		start, end int
		asc        bool
	}{
		{"whole bucket ascending", 0, -1, true},
		{"whole bucket descending", 0, -1, false},
		{"first page ascending", 0, 1, true},
		{"second page ascending", 2, 3, true},
		{"first page descending", 0, 1, false},
		{"single job ascending", 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := nodeGetJobs(t, prefix, queueName, "wait", tc.start, tc.end, tc.asc)
			got := mkqListIDs(t, ctx, queue, mkq.JobBucketWait, int64(tc.start), int64(tc.end), tc.asc)
			assert.Equal(t, want, got,
				"mkq and BullMQ TS must agree on order for getJobs(wait, %d, %d, %t)",
				tc.start, tc.end, tc.asc)
		})
	}

	// 参照側 (BullMQ TS) が本当に「古い順」を返していることも確かめる。
	// 両方が同じように壊れていたら上の比較は通ってしまう。
	assert.Equal(t, added, nodeGetJobs(t, prefix, queueName, "wait", 0, -1, true),
		"BullMQ TS itself must return oldest first; otherwise the parity check proves nothing")
}
