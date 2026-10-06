//go:build interop

package interop_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// startBullBoard spawns bullboard.js as a subprocess and returns the
// listen port once it prints {"event":"ready","port":N}. Cleanup is
// registered on t. Stdout is drained for the lifetime of the
// subprocess to avoid the OS pipe buffer deadlock pattern called out
// in PR #24's review.
func startBullBoard(t *testing.T, prefix, queueName string) int {
	t.Helper()
	dir := nodeDir(t)
	cmd := exec.Command("node", "bullboard.js")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"INTEROP_REDIS="+redisAddr(),
		"INTEROP_PREFIX="+prefix,
		"INTEROP_QUEUE="+queueName,
		"INTEROP_BULLBOARD_PORT=0",
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	type readyMsg struct {
		Event string `json:"event"`
		Port  int    `json:"port"`
	}
	ready := make(chan readyMsg, 1)
	var readyOnce sync.Once
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			var msg readyMsg
			if json.Unmarshal([]byte(line), &msg) == nil && msg.Event == "ready" {
				readyOnce.Do(func() { ready <- msg })
			}
		}
	}()

	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})

	select {
	case msg := <-ready:
		require.NotZero(t, msg.Port, "bullboard reported port 0")
		return msg.Port
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("bullboard never printed ready")
		return 0
	}
}

// TestInterop_BullBoardReadsQueueState pins that bull-board's
// BullMQ adapter can list and count jobs that mkq wrote to Redis.
// If mkq's wire shape were off, the adapter would either error
// out or report wrong counts — this is the canonical admin-UI
// compatibility smoke for Phase 4.
func TestInterop_BullBoardReadsQueueState(t *testing.T) {
	prefix := uniquePrefix(t)
	const queueName = "deliver"

	c := newClient(t, prefix)
	queue := mkq.Define[interopPayload](c, queueName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Add a delayed job (won't be picked up during this test).
	_, err := queue.Add(ctx, interopPayload{Inbox: "delayed"},
		mkq.WithDelay(60*time.Second),
	)
	require.NoError(t, err)

	// 2. Add and complete one job via mkq Process.
	completedJob, err := queue.Add(ctx, interopPayload{Inbox: "ok"})
	require.NoError(t, err)
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[interopPayload]) (any, error) {
		return map[string]any{"by": "mkq"}, nil
	}, mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)

	rdb := rawClient(t)
	base := prefix + ":" + queueName + ":"
	waitFor(t, ctx, 50*time.Millisecond, func() bool {
		v, _ := rdb.ZScore(ctx, base+"completed", completedJob.ID).Result()
		return v > 0
	})
	require.NoError(t, worker.Stop(context.Background()))

	// 3. Boot bull-board pointed at the queue we just populated.
	port := startBullBoard(t, prefix, queueName)

	// 4. bull-board の JSON API を叩く。Express アダプタは
	//    /admin/api/queues/<encoded-prefixed-queue-name>?activeQueue=...&status=... を公開するが、
	//    「キューが存在すること」の smoke には index エンドポイントが最もシンプル。
	url := fmt.Sprintf("http://127.0.0.1:%d/admin/api/queues", port)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"bull-board adapter must serve /admin/api/queues for an mkq-fed queue")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var page struct {
		Queues []struct {
			Name        string         `json:"name"`
			Counts      map[string]int `json:"counts"`
			DisplayName string         `json:"displayName"`
		} `json:"queues"`
	}
	require.NoError(t, json.Unmarshal(body, &page),
		"bull-board response must be JSON: %s", string(body))

	// 5. レスポンスから対象キューを探してカウントを検証する。bull-board は
	//    queueQualifiedName ({prefix}:{queue}) でキューを識別し、`name` または
	//    `displayName` のいずれかに表示されるため両方を確認する。
	var found bool
	var counts map[string]int
	for _, q := range page.Queues {
		if q.Name == queueName || q.DisplayName == queueName {
			found = true
			counts = q.Counts
			break
		}
	}
	require.True(t, found, "bull-board did not list queue %q in: %s", queueName, string(body))

	assert.GreaterOrEqual(t, counts["completed"], 1,
		"bull-board must report at least the one mkq-completed job; counts=%v", counts)
	assert.GreaterOrEqual(t, counts["delayed"], 1,
		"bull-board must report at least the one mkq-delayed job; counts=%v", counts)
}
