// bench is a small driver that measures mkq throughput and end-to-end
// latency against a local Redis. Pair it with bench/node/{produce,
// consume}.mjs to compare against BullMQ TS on the same workload.
//
// Modes:
//
//	produce — Add N jobs as fast as possible. Reports producer wall time.
//	consume — Process N jobs with a no-op handler. Reports worker wall time.
//	latency — produce + observe `completed` events; report p50/p99 e2e ms.
//
// Each mode writes a one-line JSON summary to stdout so run.sh can parse
// it. Logs go to stderr.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/elythia-network/mkq"
)

type payload struct {
	Inbox string `json:"inbox"`
	Body  string `json:"body"`
}

func main() {
	mode := flag.String("mode", "", "produce | consume | latency")
	jobs := flag.Int("jobs", 1000, "number of jobs to push / process")
	concurrency := flag.Int("concurrency", 16, "worker concurrency (consume / latency)")
	queueName := flag.String("queue", "bench", "queue name")
	prefix := flag.String("prefix", "mkqbench", "BullMQ keyPrefix")
	addr := flag.String("redis", "127.0.0.1:6379", "redis host:port")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	c, err := mkq.NewClient(ctx, mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{*addr}},
		KeyPrefix: *prefix,
	})
	if err != nil {
		log.Fatalf("mkq.NewClient: %v", err)
	}
	defer c.Close()

	queue := mkq.Define[payload](c, *queueName)

	switch *mode {
	case "produce":
		runProduce(ctx, queue, *jobs)
	case "consume":
		runConsume(ctx, queue, *jobs, *concurrency)
	case "latency":
		runLatency(ctx, queue, *jobs, *concurrency)
	default:
		fmt.Fprintln(os.Stderr, "missing -mode (produce | consume | latency)")
		os.Exit(2)
	}
}

func runProduce(ctx context.Context, queue *mkq.Queue[payload], n int) {
	pl := payload{Inbox: "https://example.org/inbox", Body: "hello"}

	start := time.Now()
	for i := range n {
		if _, err := queue.Add(ctx, pl); err != nil {
			log.Fatalf("Add[%d]: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	emit(addRuntime(map[string]any{
		"side":   "mkq",
		"mode":   "produce",
		"jobs":   n,
		"ms":     elapsed.Milliseconds(),
		"perSec": float64(n) / elapsed.Seconds(),
	}))
}

func runConsume(ctx context.Context, queue *mkq.Queue[payload], n int, concurrency int) {
	var processed atomic.Int64
	done := make(chan struct{})

	start := time.Now()
	worker, err := mkq.Process(queue, func(_ context.Context, _ *mkq.Job[payload]) (any, error) {
		if processed.Add(1) == int64(n) {
			close(done)
		}
		return nil, nil
	},
		mkq.WithConcurrency(concurrency),
		mkq.WithIdlePollInterval(10*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("mkq.Process: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		log.Fatalf("consume: timed out after %d/%d jobs", processed.Load(), n)
	}
	elapsed := time.Since(start)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = worker.Stop(stopCtx)

	emit(addRuntime(map[string]any{
		"side":        "mkq",
		"mode":        "consume",
		"jobs":        n,
		"concurrency": concurrency,
		"ms":          elapsed.Milliseconds(),
		"perSec":      float64(n) / elapsed.Seconds(),
	}))
}

// runLatency drives both producer and consumer in the same process,
// timestamping each job at Add and again on handler entry. The handler
// records the per-job e2e ms; we report p50 / p99 over the full set.
//
// We push once up front (so the worker doesn't bottleneck on enqueue
// rate) then start the worker; e2e then captures dispatcher polling +
// dequeue latency on top of push pipeline cost.
func runLatency(ctx context.Context, queue *mkq.Queue[payload], n int, concurrency int) {
	addedAt := make(map[string]time.Time, n)
	var addedMu sync.Mutex

	pl := payload{Inbox: "https://example.org/inbox", Body: "hello"}
	for i := range n {
		now := time.Now()
		j, err := queue.Add(ctx, pl)
		if err != nil {
			log.Fatalf("Add[%d]: %v", i, err)
		}
		addedMu.Lock()
		addedAt[j.ID] = now
		addedMu.Unlock()
	}

	latencies := make([]time.Duration, 0, n)
	var latMu sync.Mutex
	var processed atomic.Int64
	done := make(chan struct{})

	worker, err := mkq.Process(queue, func(_ context.Context, j *mkq.Job[payload]) (any, error) {
		now := time.Now()
		addedMu.Lock()
		t0, ok := addedAt[j.ID]
		addedMu.Unlock()
		if ok {
			latMu.Lock()
			latencies = append(latencies, now.Sub(t0))
			latMu.Unlock()
		}
		if processed.Add(1) == int64(n) {
			close(done)
		}
		return nil, nil
	},
		mkq.WithConcurrency(concurrency),
		mkq.WithIdlePollInterval(10*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("mkq.Process: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		log.Fatalf("latency: timed out after %d/%d jobs", processed.Load(), n)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = worker.Stop(stopCtx)

	p50, p99 := percentiles(latencies)
	emit(addRuntime(map[string]any{
		"side":        "mkq",
		"mode":        "latency",
		"jobs":        n,
		"concurrency": concurrency,
		"p50ms":       p50.Milliseconds(),
		"p99ms":       p99.Milliseconds(),
	}))
}

// addRuntime augments a result map with Go-runtime self-observed memory
// stats. These are application-internal numbers (heap-only); for OS-
// level RSS / CPU pair them with run.sh's /usr/bin/time wrapper.
//
// `allocPerJob` divides the cumulative TotalAlloc counter by the job
// count. For the produce mode this is a clean per-job figure since
// produce is essentially the only work the process does. For consume
// and latency modes the numerator includes setup + (for latency) the
// preceding enqueue phase, so the per-job number is an upper bound
// rather than a tight measurement of the dispatch path's cost. Use
// produce-mode allocPerJob when the question is "how heavy is one
// Add"; for dispatch-cost work, instrument inside the handler.
func addRuntime(m map[string]any) map[string]any {
	var ms runtime.MemStats
	runtime.GC() // make HeapAlloc measure live objects, not pre-GC peak
	runtime.ReadMemStats(&ms)
	m["heapAllocBytes"] = ms.HeapAlloc
	m["totalAllocBytes"] = ms.TotalAlloc
	m["numGC"] = ms.NumGC
	if jobs, ok := m["jobs"].(int); ok && jobs > 0 {
		m["allocPerJob"] = float64(ms.TotalAlloc) / float64(jobs)
	}
	return m
}

func percentiles(d []time.Duration) (p50, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0
	}
	slices.Sort(d)
	p50 = d[len(d)*50/100]
	p99 = d[len(d)*99/100]
	return
}

func emit(m map[string]any) {
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(m); err != nil {
		log.Fatalf("encode: %v", err)
	}
}
