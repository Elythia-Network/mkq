//go:build interop

package interop_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elythia-network/mkq"
)

// nodeWorkerOpts customises a startNodeWorker call beyond the
// defaults baked into worker.js. Pass a zero value for the basic
// happy-path worker; populate fields to opt into failure injection,
// custom concurrency, rate limiting, or extended hold time.
type nodeWorkerOpts struct {
	// Fail makes the handler always throw — useful for cross-language
	// retry / failed-state interop scenarios.
	Fail bool
	// Concurrency overrides the BullMQ Worker concurrency (default 1).
	Concurrency int
	// LimiterMax / LimiterDurationMs configure the Worker's per-worker
	// rate limiter. Both must be >0 for the limiter to apply.
	LimiterMax        int
	LimiterDurationMs int
	// LockDurationMs overrides the BullMQ Worker.lockDuration (default
	// 30s). Set short (e.g. 1s) to test lock interop scenarios.
	LockDurationMs int
	// HoldMs makes the handler sleep before returning. Combined with
	// LockDurationMs short and INTEROP_FAIL=0 it's how the test rig
	// keeps a job locked for stalled / lock interop.
	HoldMs int
}

// startNodeWorkerOpts is the option-bearing variant of
// startNodeWorker. The original startNodeWorker delegates to this
// with a zero opts struct.
func startNodeWorkerOpts(t *testing.T, prefix, queueName string, opts nodeWorkerOpts) {
	t.Helper()
	dir := nodeDir(t)
	cmd := exec.Command("node", "worker.js")
	cmd.Dir = dir
	env := append(os.Environ(),
		"INTEROP_REDIS="+redisAddr(),
		"INTEROP_PREFIX="+prefix,
		"INTEROP_QUEUE="+queueName,
	)
	if opts.Fail {
		env = append(env, "INTEROP_FAIL=1")
	}
	if opts.Concurrency > 0 {
		env = append(env, "INTEROP_CONCURRENCY="+strconv.Itoa(opts.Concurrency))
	}
	if opts.LimiterMax > 0 && opts.LimiterDurationMs > 0 {
		env = append(env, "INTEROP_LIMITER="+
			`{"max":`+strconv.Itoa(opts.LimiterMax)+`,"duration":`+strconv.Itoa(opts.LimiterDurationMs)+`}`)
	}
	if opts.LockDurationMs > 0 {
		env = append(env, "INTEROP_LOCK_DURATION_MS="+strconv.Itoa(opts.LockDurationMs))
	}
	if opts.HoldMs > 0 {
		env = append(env, "INTEROP_HOLD_MS="+strconv.Itoa(opts.HoldMs))
	}
	cmd.Env = env
	startWorkerProcess(t, cmd)
}

// runNodeEnqueuerOpts is the option-bearing variant of runNodeEnqueuer.
// Pass an empty nodeEnqueuerOpts for the basic happy-path Add.
type nodeEnqueuerOpts struct {
	// Name overrides Job.name (default: queue name).
	Name string
	// OptsJSON is the BullMQ JobsOptions object as JSON, forwarded
	// verbatim to queue.add(name, data, opts). Empty = no opts.
	OptsJSON string
}

func runNodeEnqueuerOpts(t *testing.T, prefix, queueName, payloadJSON string, opts nodeEnqueuerOpts) string {
	t.Helper()
	dir := nodeDir(t)
	cmd := exec.Command("node", "enqueuer.js")
	cmd.Dir = dir
	env := append(os.Environ(),
		"INTEROP_REDIS="+redisAddr(),
		"INTEROP_PREFIX="+prefix,
		"INTEROP_QUEUE="+queueName,
		"INTEROP_PAYLOAD="+payloadJSON,
	)
	if opts.Name != "" {
		env = append(env, "INTEROP_NAME="+opts.Name)
	}
	if opts.OptsJSON != "" {
		env = append(env, "INTEROP_OPTS="+opts.OptsJSON)
	}
	cmd.Env = env
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	require.NoError(t, err, "enqueuer.js failed")

	var msg struct {
		JobID string `json:"jobId"`
	}
	require.NoError(t, json.Unmarshal(out, &msg), "enqueuer.js stdout: %s", string(out))
	require.NotEmpty(t, msg.JobID)
	return msg.JobID
}

// runNodeInspector invokes inspector.js with a subcommand and parses
// the single-line JSON result into out. Synchronous: returns after the
// subprocess exits.
func runNodeInspector(t *testing.T, prefix, queueName string, out any, args ...string) {
	t.Helper()
	runNodeScript(t, "inspector.js", prefix, queueName, out, args...)
}

// runNodeScript is runNodeInspector for any of the single-shot harness
// scripts that take the same env and emit one JSON line.
func runNodeScript(t *testing.T, script, prefix, queueName string, out any, args ...string) {
	t.Helper()
	dir := nodeDir(t)
	cmd := exec.Command("node", append([]string{script}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"INTEROP_REDIS="+redisAddr(),
		"INTEROP_PREFIX="+prefix,
		"INTEROP_QUEUE="+queueName,
	)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.Output()
	require.NoError(t, err, "%s %v failed", script, args)
	require.NoError(t, json.Unmarshal(stdout, out), "%s %v stdout: %s", script, args, string(stdout))
}

// startWorkerProcess starts the given subprocess, drains stdout for
// its lifetime, and blocks until a JSON line `{"event":"ready"}` is
// emitted (or fails the test on timeout). Cleanup registered on t.
//
// Kept in this file so the original startNodeWorker (in
// interop_test.go) and the option-bearing startNodeWorkerOpts share
// a single ready/cleanup state machine.
func startWorkerProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	ready := make(chan struct{})
	var readyOnce sync.Once
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			var msg struct {
				Event string `json:"event"`
			}
			if json.Unmarshal([]byte(line), &msg) == nil && msg.Event == "ready" {
				readyOnce.Do(func() { close(ready) })
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
	case <-ready:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("node subprocess never printed ready")
	}
}

// startEventsListener spins up events-listener.js and returns a
// channel that receives every event line as a parsed map. The channel
// closes when the subprocess exits or t cleans up. The "ready"
// beacon is consumed internally so callers see only data events.
//
// optionalEvents narrows the listener's emission set (default all).
func startEventsListener(t *testing.T, prefix, queueName string, optionalEvents ...string) <-chan map[string]any {
	t.Helper()
	dir := nodeDir(t)
	cmd := exec.Command("node", "events-listener.js")
	cmd.Dir = dir
	env := append(os.Environ(),
		"INTEROP_REDIS="+redisAddr(),
		"INTEROP_PREFIX="+prefix,
		"INTEROP_QUEUE="+queueName,
	)
	if len(optionalEvents) > 0 {
		env = append(env, "INTEROP_EVENTS="+joinComma(optionalEvents))
	}
	cmd.Env = env
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	out := make(chan map[string]any, 32)
	ready := make(chan struct{})
	var readyOnce sync.Once

	go func() {
		defer close(out)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			var msg map[string]any
			if json.Unmarshal([]byte(line), &msg) != nil {
				continue
			}
			if e, _ := msg["event"].(string); e == "ready" {
				readyOnce.Do(func() { close(ready) })
				continue
			}
			out <- msg
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
	case <-ready:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("events-listener.js never printed ready")
	}
	return out
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

// stopWorker tears a worker down at test end.
//
// **Stop の戻り値を捨てない。** 失敗は「drain が終わらなかった」という情報で、
// goroutine と Redis 接続が残ったまま後続のテストに影響しうる。テスト自体を
// 落とすほどではないので log に出す。
//
// 本体パッケージの同名ヘルパ (queue_test.go) と同じもの。テストヘルパは
// パッケージを跨げないので、同じ意図を 2 箇所に置いている。
func stopWorker(t *testing.T, w *mkq.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Stop(ctx); err != nil {
		t.Logf("worker did not stop cleanly: %v", err)
	}
}
