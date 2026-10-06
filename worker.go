package mkq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/elythia-network/mkq/internal/keys"
	"github.com/elythia-network/mkq/internal/lua"
	"github.com/elythia-network/mkq/internal/proto"
)

// Handler is the user function invoked once per dequeued job. The
// returned value (any) is JSON-encoded and stored in the BullMQ
// `returnvalue` HASH field on success.
//
// A non-nil error transitions the job to retry or failed depending on
// the job's WithAttempts / WithBackoff configuration. Wrapping the
// returned error with ErrUnrecoverable forces the failed transition
// regardless of remaining attempts. Returning (or wrapping) a
// DelayedError instead puts the job back in delayed without consuming an
// attempt; it takes precedence over ErrUnrecoverable when both are
// wrapped.
//
// Notes mirroring BullMQ behaviour:
//   - Panics inside the handler are recovered, recorded in the
//     stacktrace HASH field, and treated as a regular error — they
//     are eligible for retry under WithAttempts. Use
//     ErrUnrecoverable inside an explicit recover() if you need
//     panics to be terminal.
//   - The `retries-exhausted` event in the BullMQ events stream
//     fires whenever a job lands in `failed` and `attemptsMade`
//     reached the configured `attempts`. With the default (no
//     WithAttempts), `attempts` is 0 and any failure satisfies the
//     gate, so the event fires immediately. This matches BullMQ TS
//     wire-format behaviour.
type Handler[T any] func(ctx context.Context, job *Job[T]) (any, error)

// Worker is the lifecycle handle returned by Process. It owns its
// goroutine pool and the per-job lock heartbeats.
type Worker struct {
	cfg       workerConfig
	keys      queueKeys
	queueName string

	// wakeKey is this Worker instance's private ZSET for shutdown
	// wake-ups. Dispatchers block on it alongside the shared marker.
	//
	// **共有 marker では特定の worker を起こせない。** marker はキュー単位で
	// 同じキューの全 worker が読むので、Stop が push した分を生き残る worker の
	// dispatcher が横取りしうる。実際に Resize で 16 → 4 に縮めたとき、止めたい
	// worker の dispatcher が起きずに Stop が返らなくなった。
	//
	// 名前は Worker インスタンスごとに一意 (uuid)。cfg.workerName は呼び出し側が
	// 指定できるので、同名の Worker を複数作られると衝突する。
	wakeKey string
	scripts *lua.Scripter
	rdb     redis.UniversalClient
	logger  Logger
	metrics Metrics
	tracer  Tracer

	// backoffStrategy is the BullMQ settings.backoffStrategy analogue:
	// invoked for jobs whose backoff Type is not a built-in (fixed /
	// exponential). nil means custom-typed jobs fall back to immediate
	// retry. See WithBackoffStrategy.
	backoffStrategy CustomBackoffFunc
	// backoffFunc is the context-aware form of the same hook. It wins
	// when both are registered.
	backoffFunc BackoffFunc
	// retryDelayOverride is consulted before either of the above, for
	// every backoff type. See WithRetryDelayOverride.
	retryDelayOverride RetryDelayFunc
	// rng returns a random float64 in [0, 1) for jitter. Defaults to
	// math/rand/v2's package source; overridable in tests for
	// deterministic jitter assertions.
	rng func() float64

	// runCtx is cancelled to break dispatch goroutines out of the
	// dequeue loop. Both Stop and Drain cancel it: neither takes new
	// work once shutdown has begun.
	runCtx    context.Context
	runCancel context.CancelFunc

	// jobsCtx is the parent of every in-flight handler's context, and
	// is deliberately *not* derived from runCtx.
	//
	// **この2つを分けているのが Drain の全部。** 同じ context を使い回すと
	// 「新規 dequeue を止める」と「実行中の handler を cancel する」が
	// 不可分になり、走っているジョブを完走させる余地が無くなる。Stop は
	// 両方を cancel し、Drain は runCtx だけを cancel する。
	jobsCtx    context.Context
	jobsCancel context.CancelFunc

	// run tracks every dispatch goroutine. Because dispatchLoop runs
	// the handler synchronously (one in-flight job per loop), this
	// also covers handler completion — no separate WaitGroup needed.
	run sync.WaitGroup

	// stopOnce / stopDone make Stop idempotent: repeated Stop calls
	// share the same waiter goroutine instead of spawning a new one
	// per call. The single waiter still blocks on w.run.Wait() and
	// exits once every dispatch loop / stalled-checker / in-flight
	// handler has returned, bounding the post-Stop reachability of
	// the Worker struct to handler-completion time.
	stopOnce sync.Once
	stopDone chan struct{}
}

// queueKeys is a snapshot of the per-queue Redis keys consumed by
// moveToActive / moveToFinished / extendLock / releaseLock. We capture
// it at Process time so the worker can run without holding a *Queue.
type queueKeys struct {
	wait, active, prioritized, events, stalled string
	limiter, delayed, paused, meta, pc, marker string
	completed, failed                          string
	metricsCompleted, metricsFailed            string
	stalledCheck                               string
	prefix                                     string
	// repeat / id are needed for the worker-side re-schedule path
	// (updateJobScheduler-12). builder retained so the worker can
	// derive per-scheduleID HASH keys without re-deriving the prefix.
	repeat, id string
	builder    keys.Builder
}

// Process starts a worker that pulls jobs from q and runs h on each.
// Process returns once the worker goroutines are up. Shut it down with
// Worker.Drain to let the in-flight jobs finish, or Worker.Stop to
// cancel them.
func Process[T any](q *Queue[T], h Handler[T], opts ...WorkerOption) (*Worker, error) {
	if q == nil {
		return nil, errors.New("mkq: Process requires a non-nil queue")
	}
	if h == nil {
		return nil, errors.New("mkq: Process requires a non-nil handler")
	}

	cfg := workerConfig{
		concurrency:      defaultConcurrency,
		lockDuration:     defaultLockDuration,
		idlePollInterval: defaultIdlePollInterval,
		stalledInterval:  defaultStalledInterval,
		maxStalledCount:  defaultMaxStalledCount,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	if cfg.workerName == "" {
		cfg.workerName = defaultWorkerName()
	}

	ctx, cancel := context.WithCancel(context.Background())
	jobsCtx, jobsCancel := context.WithCancel(context.Background())
	qk := newQueueKeys(q)
	w := &Worker{
		cfg:                cfg,
		keys:               qk,
		wakeKey:            qk.builder.Base() + "mkq:wake:" + uuid.NewString(),
		queueName:          q.name,
		scripts:            q.client.scripts,
		rdb:                q.client.rdb,
		logger:             q.client.logger,
		metrics:            q.client.metrics,
		tracer:             q.client.tracer,
		runCtx:             ctx,
		runCancel:          cancel,
		jobsCtx:            jobsCtx,
		jobsCancel:         jobsCancel,
		stopDone:           make(chan struct{}),
		backoffStrategy:    cfg.backoffStrategy,
		backoffFunc:        cfg.backoffFunc,
		retryDelayOverride: cfg.retryDelayOverride,
		rng:                rand.Float64,
	}

	// BZPopMin の awaitMarker は worker slot ごとに 1 connection を専有
	// するため、concurrency >= pool size だと dispatch が他の Redis 呼び
	// 出し (extendLock など) を待ってデッドロック気味になる。
	// configuredPoolSize=0 は go-redis の既定値 (10*GOMAXPROCS) を意味
	// するので、ここで実効値に展開してから比較する。これにより既定値
	// ユーザでも concurrency が大きい場合に警告が出る。
	//
	// 限界: この警告は Process() 1 呼び出し単体での見積もり。同一 Client
	// に複数 Worker がぶら下がる場合は累積 concurrency を見ないので
	// 警告が漏れる。Cluster mode の go-redis 既定は 5*GOMAXPROCS per
	// node なので 10*GOMAXPROCS の見積もりは過大 (= 警告が出にくい)
	// 方向に外れる。Ops 側で複数ワーカ運用 / cluster mode の場合は
	// PoolSize を明示指定して本警告のシグナルを正しく扱う前提。
	effectivePoolSize := q.client.configuredPoolSize
	if effectivePoolSize == 0 {
		effectivePoolSize = 10 * runtime.GOMAXPROCS(0)
	}
	if cfg.concurrency+8 > effectivePoolSize {
		w.logger.Warn("mkq: redis pool size below recommended (concurrency + 8)",
			slog.String(AttrQueue, q.name),
			slog.Int("concurrency", cfg.concurrency),
			slog.Int("pool_size", effectivePoolSize),
			slog.Int("recommended_minimum", cfg.concurrency+8),
		)
	}

	// 登録された strategy が黙って無視されるのは、原因を追いづらい種類の
	// 事故になる。どちらが使われるかを起動時に言っておく。
	if cfg.backoffFunc != nil && cfg.backoffStrategy != nil {
		w.logger.Warn("mkq: both WithBackoffStrategyFunc and WithBackoffStrategy are set; the context-aware one is used",
			slog.String(AttrQueue, q.name),
		)
	}

	shim := newHandlerShim(q, h)
	for i := 0; i < cfg.concurrency; i++ {
		w.run.Add(1)
		go w.dispatchLoop(shim)
	}

	// Single stalled-checker per Worker. The Lua's stalled-check key
	// TTL serialises concurrent workers automatically.
	if cfg.stalledInterval > 0 {
		w.run.Add(1)
		go w.stalledLoop()
	}

	return w, nil
}

// Stop signals the worker to stop dequeueing, cancels the handlers that
// are already running, and waits for them to return. If ctx is
// cancelled before they do, Stop returns ctx.Err(); any subsequent
// moveToFinished call may fail with a lock-mismatch error (logged, not
// surfaced).
//
// Use Drain instead to let the in-flight jobs finish rather than
// cancelling them.
//
// Stop is idempotent: subsequent calls block on the same internal
// channel as the first, so a graceful-stop helper invoked from
// multiple sites won't spawn extra waiter goroutines.
func (w *Worker) Stop(ctx context.Context) error {
	// **cancel が先。** beginShutdown は wakeDispatchers で Redis を叩き、
	// 相手が固まっていると最大 1 秒待つ。Redis が死んでいるから止めている、
	// という場面がまさにそれなので、その裏で handler を放置してはいけない。
	// 2 つの cancel は独立なので順序を入れ替えても他に影響はない。
	w.jobsCancel()
	w.beginShutdown()
	select {
	case <-w.stopDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Drain stops dequeueing and waits for the handlers already running to
// finish on their own. Unlike Stop it does not cancel them: an
// in-flight job keeps its context, keeps its lock extended by the
// heartbeat, and finishes normally.
//
// **これが要るのは、送信途中で切られたジョブが必ず再送になるから。** Stop で
// 落とすと handler は cancel され、掴んでいたジョブは lock TTL が切れるまで
// active に残り、stalled detection が回収して再試行する。BullMQ の
// at-least-once 仕様どおりで正しさは保たれるが、配送系では「相手には届いて
// いたのにもう一度送る」がデプロイのたびに出る。
//
// ctx bounds the wait. If it expires first, Drain cancels the running
// handlers and returns ctx.Err() straight away — it does not wait for
// them to unwind, because the budget the caller gave it is already
// spent. Follow it with Stop on a fresh context to wait for them:
//
//	if err := worker.Drain(graceCtx); err != nil {
//		// 猶予切れ。cancel は済んでいるので、巻き取りだけ待つ。
//		_ = worker.Stop(unwindCtx)
//	}
//
// **戻ってきた直後に Redis クライアントを閉じてはいけない。** handler が
// finalise (moveToFinished) を撃つのはそこからなので、閉じるとジョブが
// active に lock されたまま残り、stalled recovery が再配送する — Drain が
// 避けたかったことがそのまま起きる。
//
// Drain and Stop share the same shutdown latch, so calling Stop after
// Drain (or either one twice) is safe: the first call starts the
// shutdown and every caller waits on the same channel.
func (w *Worker) Drain(ctx context.Context) error {
	w.beginShutdown()
	select {
	case <-w.stopDone:
		return nil
	case <-ctx.Done():
		w.jobsCancel()
		return ctx.Err()
	}
}

// beginShutdown stops the dispatchers and starts the single waiter
// goroutine. It is idempotent; the job contexts are left to the caller
// so that Stop and Drain can differ on exactly that point.
func (w *Worker) beginShutdown() {
	w.stopOnce.Do(func() {
		w.runCancel()
		w.wakeDispatchers()
		go func() {
			w.run.Wait()
			// 全部戻ったら job ctx tree も畳む。Drain が成功した場合は
			// ここを通らないと cancel node が worker の寿命ぶん残る。
			w.jobsCancel()
			w.dropWakeKey()
			close(w.stopDone)
		}()
	})
}

// maxIdleWait caps how long an idle dispatcher parks on the marker key.
//
// **上限があるのは marker を取り逃したときの保険。** push は Lua 側で行われる
// ので通常は取りこぼさないが、Redis のフェイルオーバー等で欠けた場合はこの
// 間隔で気づく。stalled-recovery (既定 30 秒) と同じ桁に置いてある。
const maxIdleWait = 30 * time.Second

// markerWaitFloor is the shortest wait that awaitMarker can actually
// honour.
//
// **go-redis は BZPopMin のタイムアウトを 1 秒へ切り上げる。** これより短く
// 待ちたい場合 (sub-second の retry backoff 等) は marker 待ちに載せると
// overshoot するので、precise sleep を使わなければならない。
const markerWaitFloor = time.Second

// nextIdleWait doubles the marker wait after an idle wake-up, capped at
// maxIdleWait. base が上限を超えている場合は base を尊重する (運用者が明示
// 設定した値を勝手に縮めない)。
func nextIdleWait(cur, base time.Duration) time.Duration {
	if base >= maxIdleWait {
		return base
	}
	next := cur * 2
	if next > maxIdleWait {
		next = maxIdleWait
	}
	return next
}

// wakeDispatchers pushes to this Worker's private wake key so every
// dispatcher parked in BZPopMin returns immediately instead of waiting
// out its timeout.
//
// **ctx cancellation does not interrupt an in-flight BZPopMin.** go-redis
// sets a read deadline from the block timeout and does not abort the
// pending read when the context is cancelled, so without this nudge Stop
// takes up to the dispatcher's current wait (measured: 736ms at 1s,
// 7.78s at 8s, up to maxIdleWait once the idle backoff has grown).
//
// **共有 marker には押さない。** marker はキュー単位なので、同じキューで
// 動き続ける別 worker の dispatcher が起き上がって push を消費してしまう。
// 実際に Resize で inbox を 16 → 4 に縮めたとき、止めたい worker の
// dispatcher が起きず Stop が返らなくなり、起動が完了しなくなった。
// wakeKey はこの Worker インスタンス専用なので横取りされない。
//
// One push wakes one blocked BZPopMin, so push once per dispatcher.
// メンバー名は一意にする (sorted set なので同名は 1 件にまとまり、
// 起こせる dispatcher が 1 つだけになる)。ジョブ実行中などで
// BZPopMin にいない dispatcher の分は余るが、key ごと後で消すので
// 残らない。
//
// Best-effort by design — Stop must not fail because Redis is gone. The
// timeout path still bounds the wait, it is just slower.
func (w *Worker) wakeDispatchers() {
	if w.rdb == nil || w.wakeKey == "" {
		return
	}
	n := w.cfg.concurrency
	if n < 1 {
		n = 1
	}
	// runCtx is already cancelled here, so use a fresh bounded context.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	members := make([]redis.Z, n)
	for i := range members {
		members[i] = redis.Z{Score: 0, Member: "stop:" + strconv.Itoa(i)}
	}
	_ = w.rdb.ZAdd(ctx, w.wakeKey, members...).Err()
}

// dropWakeKey removes this Worker's private wake key once every
// dispatcher has exited. 起こしきれずに余ったメンバーが残るので、
// Worker が消えたあとに孤児の key を置き去りにしない。
func (w *Worker) dropWakeKey() {
	if w.rdb == nil || w.wakeKey == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = w.rdb.Del(ctx, w.wakeKey).Err()
}

// prefetchedJob is what moveToFinished's fetchNext branch returns:
// the next job already locked under the same token the worker just
// used for the finishing job. The dispatcher consumes it on the next
// loop iteration in lieu of calling moveToActive — saves one Redis
// round-trip per processed job in steady state, halving the
// dispatch-side EVALSHA load that caps consumer throughput.
//
// The token is reused across the prefetch handoff because BullMQ's
// fetchNextJob lua re-locks the new job with the opts.token we
// passed to moveToFinished; the worker side keeps that token alive
// via processJob's heartbeat for the new job.
type prefetchedJob struct {
	jobMap map[string]string
	jobID  string
	token  string
}

// dispatchLoop is one slot of the goroutine pool. It repeatedly tries
// to dequeue a job and process it; on empty pulls it picks the
// tightest available wakeup signal:
//
//   - prefetched job already in hand (from previous moveToFinished
//     fetchNext): consume directly, skip moveToActive
//   - rate-limited (Lua returned a cooldown ms): precise sleep
//   - delayed work scheduled (Lua returned the next-due ms): precise
//     sleep until that timestamp (BZPopMin's 1s floor would overshoot
//     sub-second delayed targets like exponential-backoff retries)
//   - truly idle: BZPopMin on the BullMQ marker key, waking ms after
//     a new wait/prioritized job lands
//
// Exits when runCtx is cancelled.
func (w *Worker) dispatchLoop(handlerAny any) {
	defer w.run.Done()
	var prefetched *prefetchedJob
	defer func() {
		// shutdown race: fetchNextFlag may have returned "1" between
		// our last ctx check and the EVAL, leaving us with a locked
		// prefetched job we're about to drop. Release the lock so it
		// doesn't sit in active for the full lockDuration +
		// stalledInterval until stalled-recovery picks it up.
		w.releasePrefetchedLock(prefetched)
	}()
	// アイドルが続くあいだ marker 待ちの上限を伸ばす。**ジョブ取得は遅く
	// ならない** — 待っているのは marker への push で、ジョブが積まれた時点で
	// Lua が push するので即座に起きる。伸ばして効くのは「空振りで起きて
	// tryOnce を撃つ」頻度だけ。
	idleWait := w.cfg.idlePollInterval
	for {
		if w.runCtx.Err() != nil {
			// **Drain のときは prefetch 済みを取りこぼさない。** 直前の
			// moveToFinished が fetchNext=1 で返ってきていると、すでに
			// wait->active へ移して lock まで取ったジョブを手元に抱えて
			// いる。ここで捨てると active に置き去りになり、stalled
			// recovery が拾うまで止まる — しかも beginShutdown は自分の
			// stalledLoop も同時に落としている。新規を取らないだけの
			// Drain の約束にも反する。
			//
			// 連鎖はしない: runCtx が cancel 済みなら fetchNextFlag が
			// "0" を返すので、この 1 件を終えたら prefetched は nil になる。
			//
			// Stop (jobsCtx も cancel 済み) では今までどおり捨てる。handler は
			// どうせ cancel 済み ctx を受け取るだけで、試行を 1 回無駄に
			// 消費するより stalled recovery に任せたほうがよい。
			if prefetched == nil || w.jobsCtx.Err() != nil {
				return
			}
		}
		if prefetched != nil {
			// Consume the prefetched job from the previous moveToFinished
			// fetchNext. runJob covers both the defa fast-path and the
			// regular handler path; the returned prefetched fuels the
			// next iteration if the chain keeps producing.
			pj := prefetched
			prefetched = nil
			prefetched = w.runJob(handlerAny, pj.token, pj.jobID, pj.jobMap)
			continue
		}
		processed, gotPrefetched, expireTimeMs, nextDelayedTs, err := w.tryOnce(handlerAny)
		if err != nil {
			// 取得や lua 失敗は Warn でログ (worker は止めない)。
			// stalledLoop と同じ best-effort パターン: 単発失敗は次 tick
			// で recover できるが、連続失敗時に黙ってジョブが滞留する
			// ことを避けるため Logger 経由で ops に出す。
			// エラー時は marker を待たずに idle interval だけ眠る:
			// 連続失敗時に hot-loop しないため。
			w.logger.Warn("mkq: dispatch attempt failed",
				slog.String(AttrQueue, w.queueName),
				slog.String(AttrError, err.Error()),
			)
			if !w.sleep(w.cfg.idlePollInterval) {
				return
			}
			continue
		}
		if processed {
			// 仕事があったので待ちを最短に戻す。
			idleWait = w.cfg.idlePollInterval
			prefetched = gotPrefetched
			continue
		}
		if expireTimeMs > 0 {
			// Rate-limited: precise cooldown the Lua reported. Don't
			// short-circuit on marker — we'd hammer moveToActive with
			// rate-limit-rejected calls.
			if !w.sleep(time.Duration(expireTimeMs) * time.Millisecond) {
				return
			}
			continue
		}
		if nextDelayedTs > 0 {
			// 次 delayed job の絶対 ms timestamp が分かるので、その時刻
			// まで待つ。delay <= 0 はもう due なので即時 retry。
			delay := time.Until(time.UnixMilli(nextDelayedTs))
			if delay <= 0 {
				continue
			}
			// sub-second (e.g. 50ms の exp-backoff retry) は marker 待ちに
			// 載せられない。BZPopMin の 1 秒床で overshoot するため。
			if delay < markerWaitFloor {
				if !w.sleep(delay) {
					return
				}
				continue
			}
			// **長い delay は marker 待ちで潰す。** 以前はここを
			// idlePollInterval (既定 100ms) で刻んでいたが、そうすると
			// 1 時間後の delayed job が 1 件あるだけで dispatcher が
			// 毎秒 10 回 tryOnce を撃ち続ける (worker 8 個で毎秒 549
			// コマンド。空のキューなら 19)。marker 待ちなら新しい
			// wait-list job は push で即座に起こせるので、刻む理由が無い。
			if delay > maxIdleWait {
				delay = maxIdleWait
			}
			if !w.awaitMarker(delay) {
				return
			}
			continue
		}
		// Idle queue: wake on marker push or timeout.
		if !w.awaitMarker(idleWait) {
			return
		}
		idleWait = nextIdleWait(idleWait, w.cfg.idlePollInterval)
	}
}

// sleep blocks for d or until runCtx is cancelled. Returns false if
// runCtx fired (caller should exit).
func (w *Worker) sleep(d time.Duration) bool {
	if d <= 0 {
		return w.runCtx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-w.runCtx.Done():
		return false
	}
}

// awaitMarker blocks waiting for a push to the BullMQ marker key
// (`{prefix}:{queue}:marker`). The vendored add* / promote /
// moveToFinished Lua scripts add to the marker whenever new work
// becomes available, so this wakes within ms of a job landing — far
// tighter than the previous polling-and-sleep approach.
//
// Redis BZPOPMIN's protocol-level timeout is second-granularity
// (Redis 6+ accepts fractional seconds, but go-redis floors anything
// below 1s with a warning log); we therefore clamp the BZPopMin
// timeout to at least 1s. Sub-second responsiveness still works in
// the only case that matters: a real job landing fires a marker
// push that wakes BZPopMin within ms regardless of the configured
// ceiling.
//
// **ctx cancellation does NOT interrupt an in-flight BZPopMin.** go-redis
// derives the read deadline from the block timeout and does not abort the
// pending read when the parent context is cancelled, so a cancelled
// runCtx alone leaves the dispatcher parked until the timeout expires
// (measured: 736ms at a 1s ceiling, 7.78s at 8s). Worker.Stop therefore
// nudges the marker key — see wakeMarkerWaiters. An earlier version of
// this comment claimed the opposite; it was wrong.
//
// Any error response (redis.Nil for timeout, context.Canceled for
// shutdown, transient network glitches) becomes a "wake and retry"
// signal: tryOnce is idempotent so spurious wakeups are cheap, and a
// real Redis outage will surface in the next tryOnce call. Returns
// false when runCtx is cancelled so the dispatcher can exit cleanly.
//
// Connection-pool sizing note: each worker slot occupies one Redis
// connection while blocked here. A WithConcurrency(N) worker plus
// the queue's own per-call traffic typically wants pool size >= N+8.
// go-redis defaults to 10*GOMAXPROCS which is comfortable for
// concurrencies up to a few dozen; very high concurrency callers
// should bump UniversalOptions.PoolSize.
func (w *Worker) awaitMarker(timeout time.Duration) bool {
	if timeout <= 0 {
		return w.runCtx.Err() == nil
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	// **専用 wake key も一緒に待つ。** marker はキュー共有なので、Stop の
	// 起こしを同じキューの別 worker に横取りされる。wakeKey はこの Worker
	// インスタンス専用なので、押した分だけ確実にこの worker が起きる。
	// marker を先に置くのは、仕事がある場合をそちらで拾わせるため。
	_ = w.rdb.BZPopMin(w.runCtx, timeout, w.keys.marker, w.wakeKey).Err()
	return w.runCtx.Err() == nil
}

// stalledLoop fires moveStalledJobsToWait at the configured interval.
// One goroutine per Worker; concurrent workers serialise via the
// Lua's stalled-check key TTL.
func (w *Worker) stalledLoop() {
	defer w.run.Done()
	t := time.NewTicker(w.cfg.stalledInterval)
	defer t.Stop()
	for {
		select {
		case <-w.runCtx.Done():
			return
		case <-t.C:
			if err := w.runStalledCheck(); err != nil {
				// Best-effort: a single tick's failure is recoverable
				// (next tick will try again). Surface to ops via Logger
				// so persistent failures don't stay invisible.
				w.logger.Warn("mkq: stalled-jobs scan failed",
					slog.String(AttrQueue, w.queueName),
					slog.String(AttrError, err.Error()),
				)
				continue
			}
		}
	}
}

// runStalledCheck invokes moveStalledJobsToWait once.
func (w *Worker) runStalledCheck() error {
	now := time.Now().UnixMilli()
	maxCheckMs := w.cfg.stalledInterval.Milliseconds()
	keys := w.keys.moveStalledKeys()
	_, err := w.scripts.Run(
		w.runCtx,
		lua.MoveStalledJobsToWait,
		keys,
		w.cfg.maxStalledCount,
		w.keys.prefix,
		now,
		maxCheckMs,
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("moveStalledJobsToWait: %w", err)
	}
	return nil
}

// tryOnce performs one dequeue attempt and, if successful, runs the
// handler and finalises the job. Returns (processed, prefetched,
// expireTimeMs, nextDelayedTs, err):
//   - processed   — true if a job was dequeued and handed to runJob
//   - prefetched  — the job moveToFinished's fetchNext returned for
//     the dispatcher to consume on the next iteration; nil for retry
//     paths (retryJob / moveToDelayed don't have fetchNext)
//   - expireTimeMs — rate-limit cooldown the dispatcher should sleep
//     before its next call (0 = no rate-limit window)
//   - nextDelayedTs — absolute ms timestamp of the soonest-due
//     delayed job (0 = none queued) so the dispatcher can wake
//     precisely on its target instead of overshooting via BZPopMin's
//     1-second floor
func (w *Worker) tryOnce(handlerAny any) (bool, *prefetchedJob, int64, int64, error) {
	token := uuid.NewString()
	now := time.Now().UnixMilli()

	mtaOpts := proto.MoveToActiveOpts{
		Token:        token,
		LockDuration: w.cfg.lockDuration.Milliseconds(),
		Name:         w.cfg.workerName,
	}
	if l := w.cfg.limiter; l != nil {
		mtaOpts.Limiter = &proto.MoveToActiveLimiter{
			Max:        l.max,
			DurationMs: l.duration.Milliseconds(),
		}
	}
	optsBytes, err := proto.EncodeMoveToActiveOpts(mtaOpts)
	if err != nil {
		return false, nil, 0, 0, fmt.Errorf("encode moveToActive opts: %w", err)
	}

	res, err := w.scripts.Run(
		w.runCtx,
		lua.MoveToActive,
		w.keys.moveToActiveKeys(),
		w.keys.prefix,
		now,
		optsBytes,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return false, nil, 0, 0, nil
		}
		return false, nil, 0, 0, fmt.Errorf("moveToActive: %w", err)
	}

	jobMap, jobID, ok, expireTimeMs, nextDelayedTs, err := parseMoveToActiveResult(res)
	if err != nil {
		return false, nil, 0, 0, fmt.Errorf("parse moveToActive: %w", err)
	}
	if !ok {
		return false, nil, expireTimeMs, nextDelayedTs, nil
	}

	prefetched := w.runJob(handlerAny, token, jobID, jobMap)
	return true, prefetched, 0, 0, nil
}

// runJob handles one acquired job: defa fast-path or normal handler
// dispatch. Returns the prefetched job moveToFinished's fetchNext
// returned (or nil if the path didn't go through moveToFinished, e.g.
// retry-immediate / retry-delayed).
//
// `defa` (default failed reason) is set by moveStalledJobsToWait
// when a job has been reclaimed more than maxStalledCount times.
// BullMQ's worker contract is to skip the handler and finalise
// straight to failed; running the handler again would re-stall the
// job in an infinite loop. moveToFinished's failed branch HDELs the
// field so a re-acquired but recovered job won't short-circuit on
// stale state.
func (w *Worker) runJob(handlerAny any, token, jobID string, jobMap map[string]string) *prefetchedJob {
	w.metrics.GaugeAdd(MetricJobsInFlight, 1, slog.String(AttrQueue, w.queueName))
	defer w.metrics.GaugeAdd(MetricJobsInFlight, -1, slog.String(AttrQueue, w.queueName))

	if defa, hasDefa := jobMap["defa"]; hasDefa && defa != "" {
		jobOpts := parseJobOpts(jobMap["opts"])
		// stalled 上限超過も 1 回の失敗として履歴に積む。
		prefetched, err := w.finishFailed(jobID, token, jobOpts, defa, buildFailureHistory(jobMap, defa))
		if err != nil {
			// Best-effort lock cleanup matches the processJob path.
			_, _ = w.scripts.Run(
				context.Background(),
				lua.ReleaseLock,
				[]string{w.keys.jobLock(jobID)},
				token,
				w.cfg.lockDuration.Milliseconds(),
			)
		} else {
			// defa fast-path は handler を呼ばずに failed に直行するので
			// processed_total は記録するが handler_duration は記録しない。
			w.metrics.CounterAdd(MetricJobsProcessedTotal, 1,
				slog.String(AttrQueue, w.queueName),
				slog.String(AttrJobName, jobMap["name"]),
				slog.String(AttrProcessStatus, "failed"),
			)
		}
		return prefetched
	}
	return w.processJob(handlerAny, token, jobID, jobMap)
}

// processJob runs the handler for one acquired job, manages the lock
// heartbeat, and writes the terminal state via the appropriate
// BullMQ Lua script (moveToFinished / retryJob / moveToDelayed).
//
// Returns the prefetched job from moveToFinished's fetchNext branch
// (nil if the outcome went through retryJob / moveToDelayed which
// don't support fetchNext). Caller (dispatchLoop) consumes the
// prefetched job on its next iteration without a moveToActive RTT.
//
// The per-job ctx is derived from jobsCtx, which Stop cancels and Drain
// does not — that is what lets a drained worker run its in-flight jobs
// to completion while taking no new ones. Heartbeat owns its own
// goroutine because the BullMQ extendLock semantics require periodic
// ticks independent of handler progress.
func (w *Worker) processJob(handlerAny any, token, jobID string, jobMap map[string]string) *prefetchedJob {
	jobCtx, cancelJob := context.WithCancel(w.jobsCtx)
	defer cancelJob()

	jobName := jobMap["name"]
	// dispatch_wait は Queue.Add 時の timestamp から handler 起動直前
	// までの end-to-end 遅延。delayed job では意図された delay 期間も
	// 含まれる (= 「ジョブが完了するまでにかかった全体時間」のうち
	// handler 実行を除いた部分)。標準ジョブでは wait-list 滞留時間に
	// 一致する。delayed の意図的な遅延を除いた純粋な滞留だけを見たい
	// 場合は、ユーザ側 ops で delay を控除する想定。
	if tsMs, perr := strconv.ParseInt(jobMap["timestamp"], 10, 64); perr == nil && tsMs > 0 {
		waitSec := float64(time.Now().UnixMilli()-tsMs) / 1000.0
		if waitSec >= 0 {
			w.metrics.HistogramObserve(MetricDispatchWaitSeconds, waitSec,
				slog.String(AttrQueue, w.queueName),
			)
		}
	}

	jobCtx, span := w.tracer.Start(jobCtx, SpanWorkerProcess,
		slog.String(AttrQueue, w.queueName),
		slog.String(AttrJobID, jobID),
		slog.String(AttrJobName, jobName),
		slog.String(AttrJobAttempts, jobMap["atm"]),
	)
	defer span.End()

	hbDone := make(chan struct{})
	go w.heartbeat(jobCtx, hbDone, token, jobID, cancelJob)

	handlerStart := time.Now()
	outcome := w.runHandler(jobCtx, handlerAny, jobID, jobMap)
	w.metrics.HistogramObserve(MetricHandlerDurationSeconds,
		time.Since(handlerStart).Seconds(),
		slog.String(AttrQueue, w.queueName),
		slog.String(AttrJobName, jobName),
	)

	// Heartbeat must finish before any finalisation Lua call so its
	// extendLock won't race with the script's lock-token validation.
	cancelJob()
	<-hbDone

	prefetched, status, err := w.finalise(jobID, token, jobMap, outcome)
	if err != nil {
		// Best-effort lock cleanup so a failed terminal call doesn't
		// strand the lock for the full TTL.
		_, _ = w.scripts.Run(
			context.Background(),
			lua.ReleaseLock,
			[]string{w.keys.jobLock(jobID)},
			token,
			w.cfg.lockDuration.Milliseconds(),
		)
		// Logger と span の両方に流す。Tracer 未設定で Logger だけの
		// ユーザでも infra エラーが拾えるようにする (stalledLoop と
		// 同じパターン)。
		w.logger.Warn("mkq: finalise failed",
			slog.String(AttrQueue, w.queueName),
			slog.String(AttrJobID, jobID),
			slog.String(AttrError, err.Error()),
		)
		// span.SetError は actionable な infra エラーで上書きするが、
		// handler が先に失敗して finishFailed が二次的に失敗した場合は
		// 元の handler error が trace から消える。debug 用に attribute
		// として残しておく (errReason は既に Lua へ渡されているので
		// 個別フィールド化しても情報重複にならない)。
		if !outcome.success && outcome.errReason != "" {
			span.SetAttrs(slog.String("mkq.handler.error", outcome.errReason))
		}
		span.SetError(err)
	}
	switch status {
	case finaliseStatusCompleted:
		w.metrics.CounterAdd(MetricJobsProcessedTotal, 1,
			slog.String(AttrQueue, w.queueName),
			slog.String(AttrJobName, jobName),
			slog.String(AttrProcessStatus, "completed"),
		)
	case finaliseStatusFailed:
		w.metrics.CounterAdd(MetricJobsProcessedTotal, 1,
			slog.String(AttrQueue, w.queueName),
			slog.String(AttrJobName, jobName),
			slog.String(AttrProcessStatus, "failed"),
		)
		span.SetError(outcome.err)
	}

	// 周期スケジュール (rjk) が紐付いている job は、仕上げ後に
	// 次 iteration をキューに積む。BullMQ TS と同じく
	// updateJobScheduler-12 を 1 度叩いて HASH の every / ic /
	// limit を読みつつ delayed に挿入する。
	if rjk := jobMap["rjk"]; rjk != "" {
		w.rescheduleNext(rjk, jobID)
	}

	return prefetched
}

// handlerOutcome captures the result of invoking a handler. Exactly
// one of returnValue / errReason is populated. stacktrace mirrors
// BullMQ's HASH `stacktrace` JSON-array shape and is set on every
// failure (panics include the captured Go stack; plain errors carry
// the message itself).
type handlerOutcome struct {
	success     bool
	returnValue string // JSON-encoded user return, success=true only
	err         error  // raw error from the handler, success=false only
	errReason   string // BullMQ failedReason (plain string)
	stacktrace  string // BullMQ stacktrace (JSON array string)
}

// runHandler invokes the user handler under panic recovery and returns
// the BullMQ-shaped wire fields. BullMQ's wire format is asymmetric:
//
//   - returnvalue is JSON.stringify-d (opaque user return).
//   - failedReason is the raw err.message string (no JSON quoting).
//   - stacktrace is JSON.stringify of an array of strings.
//
// Mirroring that asymmetry is required for bull-board and other
// foreign readers to render values without spurious quotes.
func (w *Worker) runHandler(ctx context.Context, handlerAny any, jobID string, jobMap map[string]string) (out handlerOutcome) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			reason := fmt.Sprintf("panic: %v\n%s", r, stack)
			out = handlerOutcome{
				success:    false,
				err:        fmt.Errorf("mkq: handler panic: %v", r),
				errReason:  reason,
				stacktrace: mustJSONString([]string{reason}),
			}
		}
	}()

	ret, err := invokeHandler(ctx, handlerAny, jobID, jobMap)
	if err != nil {
		return handlerOutcome{
			success:    false,
			err:        err,
			errReason:  err.Error(),
			stacktrace: mustJSONString([]string{err.Error()}),
		}
	}
	return handlerOutcome{
		success:     true,
		returnValue: mustJSONString(ret),
	}
}

// heartbeat extends the job's lock at lockDuration/2 intervals until
// the per-job ctx is cancelled or the extend call fails. A failed
// extend cancels the job ctx (cancelJob) so the handler observes a
// cancellation and finishes promptly.
func (w *Worker) heartbeat(ctx context.Context, done chan<- struct{}, token, jobID string, cancelJob context.CancelFunc) {
	defer close(done)
	tick := max(w.cfg.lockDuration/2, time.Second)
	t := time.NewTicker(tick)
	defer t.Stop()
	keys := []string{w.keys.jobLock(jobID), w.keys.stalled}
	lockMs := w.cfg.lockDuration.Milliseconds()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			res, err := w.scripts.Run(context.Background(), lua.ExtendLock, keys, token, lockMs, jobID)
			if err != nil {
				cancelJob()
				return
			}
			if n, ok := res.(int64); ok && n == 0 {
				// Lock no longer ours (stolen via stalled detection or
				// flushed). Cancel the handler so it doesn't keep
				// running off-lock.
				cancelJob()
				return
			}
		}
	}
}

// rescheduleNext queues the next iteration of a recurring schedule via
// updateJobScheduler-12.lua, gated by the schedule HASH's `limit`
// counter (BullMQ does not enforce limit Lua-side, so the worker
// short-circuits before invoking the script when the cap is reached).
//
// The lua itself reads the schedule template (every, name, data, etc.)
// directly out of the HASH, increments `ic`, and inserts a single new
// instance into the delayed ZSET. If RemoveSchedule has dropped the
// HASH between dequeue and finalise, the lua's prevMillis check no-ops
// silently.
func (w *Worker) rescheduleNext(scheduleID, currentJobID string) {
	scheduleKey := w.keys.builder.Schedule(scheduleID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// limit=0 (unset) は cap なし。limit>0 かつ ic>=limit で打ち止め。
	// every / pattern のいずれかが必須 (両者とも未設定なら HASH が
	// 消えている = no-op で終了)。HMGET は scheduler が消えていれば
	// nil を返すので、その場合は startDate/endDate も nil で安全。
	// **`opts` も読む。** scheduler HASH の `opts` は各 iteration が継承する
	// job opts (retention 等) で、BullMQ TS の Worker も再スケジュール時に
	// ここを読んで次の opts を組み立てている。読まずに repeat だけで組み直すと
	// 1 回目の iteration にしか retention が載らない。
	vals, err := w.rdb.HMGet(ctx, scheduleKey,
		"limit", "ic", "every", "startDate", "endDate", "pattern", "tz", "opts",
	).Result()
	if err != nil || len(vals) != 8 {
		return
	}
	limit := parseInt(asString(vals[0]))
	ic := parseInt(asString(vals[1]))
	if limit > 0 && ic >= limit {
		return
	}
	everyMs, _ := strconv.ParseInt(asString(vals[2]), 10, 64)
	startDate, _ := strconv.ParseInt(asString(vals[3]), 10, 64)
	endDate, _ := strconv.ParseInt(asString(vals[4]), 10, 64)
	pattern := asString(vals[5])
	tz := asString(vals[6])
	template, err := proto.DecodeScheduleTemplateOpts(asString(vals[7]))
	if err != nil {
		// HASH の opts が壊れている = データ破損 or 別 client が書き換えた。
		// 空の template で積み直すことはできるが、attempts / priority が
		// 黙って消えるので ops に見せる。次 iteration は出す。
		w.logger.Error("mkq: scheduler template opts are unreadable; the next iteration loses them",
			slog.String(AttrQueue, w.queueName),
			slog.String("schedule_id", scheduleID),
			slog.String(AttrError, err.Error()),
		)
	}

	if everyMs <= 0 && pattern == "" {
		// HASH が消えた、または不完全な状態。lua の prevMillis check に
		// 任せても良いが、ここで早期 return しておけば余計な EVAL を省ける。
		return
	}

	scheduleProto := proto.ScheduleOpts{
		EveryMs:   everyMs,
		Pattern:   pattern,
		TZ:        tz,
		StartDate: startDate,
		EndDate:   endDate,
		Limit:     limit,
	}
	delayedOpts, err := proto.EncodeScheduleDelayedOpts(scheduleProto, template)
	if err != nil {
		return
	}

	// pattern-mode は Go 側で次 millis を計算 → ARGV[1]。
	// every-mode は "0" を渡し、lua が getJobSchedulerEveryNextMillis
	// で計算する。every-mode の場合は endDate ガードを行うために
	// nextMillis を Go 側でも算出する (lua に任せるとガードできない)。
	nowTime := time.Now()
	now := nowTime.UnixMilli()
	nextMillisArg := "0"
	var nextMillisVal int64
	switch {
	case pattern != "":
		cc, err := parseCron(pattern, tz)
		if err != nil {
			// HASH に書かれている pattern が parse できない = データ
			// 破損 or 別 client が書き換えた。non-fatal だが次 iteration
			// が永久に出ないので ops 通知が必要 → Error level。
			w.logger.Error("mkq: cron pattern parse failure in rescheduleNext",
				slog.String(AttrQueue, w.queueName),
				slog.String("schedule_id", scheduleID),
				slog.String("pattern", pattern),
				slog.String("tz", tz),
				slog.String(AttrError, err.Error()),
			)
			return
		}
		nextMillisVal = cc.nextFire(nowTime).UnixMilli()
		nextMillisArg = strconv.FormatInt(nextMillisVal, 10)
	default: // every-mode
		// getJobSchedulerEveryNextMillis のロジック: 前回 fire 時刻 +
		// every。前回 fire 時刻が無い (初回) なら startDate or now。
		// rescheduleNext は worker が job を完了した直後に呼ばれるので、
		// 必ず prevMillis が ZSCORE で取れる前提だが、endDate ガード
		// 用途では now + every で近似すれば十分。
		nextMillisVal = now + everyMs
	}
	if endDate > 0 && nextMillisVal > endDate {
		// BullMQ TS は worker.getNextMillis() で同様の endDate チェックを
		// 行っており、超過した場合は新しい iteration を作らない。
		return
	}
	keysArg := []string{
		w.keys.repeat,      // KEYS[1]  repeat
		w.keys.delayed,     // KEYS[2]  delayed
		w.keys.wait,        // KEYS[3]  wait
		w.keys.paused,      // KEYS[4]  paused
		w.keys.meta,        // KEYS[5]  meta
		w.keys.prioritized, // KEYS[6]  prioritized
		w.keys.marker,      // KEYS[7]  marker
		w.keys.id,          // KEYS[8]  id
		w.keys.events,      // KEYS[9]  events
		w.keys.pc,          // KEYS[10] pc
		"",                 // KEYS[11] producer key (unused)
		w.keys.active,      // KEYS[12] active
	}

	_, _ = w.scripts.Run(
		ctx,
		lua.UpdateJobScheduler,
		keysArg,
		nextMillisArg, // ARGV[1] nextMillis: every="0" / pattern=Go計算値
		scheduleID,    // ARGV[2]
		"{}",          // ARGV[3] data fallback (HASH 優先)
		delayedOpts,   // ARGV[4] msgpack delayed opts (repeat.every / pattern 含む)
		now,           // ARGV[5]
		w.keys.prefix, // ARGV[6]
		currentJobID,  // ARGV[7] producerId — current job ID で dedupe
	)
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// finalise dispatches the terminal-state Lua call for a job. On
// success it always calls moveToFinished("completed"). On failure it
// consults retry policy and may instead call retryJob (immediate
// re-enqueue) or moveToDelayed (backoff-delayed re-enqueue), bumping
// the BullMQ HASH `atm` (attempts made) counter atomically inside
// the script.
//
// finaliseStatus distinguishes the five post-finalise states the
// caller cares about for observability. completed/failed are terminal;
// retrying means the job got handed back to delayed/wait for another
// attempt; delayed means the handler asked to be run later (DelayedError)
// without consuming an attempt; finaliseError means the script call
// itself failed.
type finaliseStatus int

const (
	finaliseStatusCompleted finaliseStatus = iota
	finaliseStatusFailed
	finaliseStatusRetrying
	finaliseStatusDelayed
	finaliseStatusError
)

// Returns the prefetched job from moveToFinished's fetchNext branch
// when the path goes through finishCompleted / finishFailed. Retry
// paths (retryJob / moveToDelayed) never produce a prefetched job
// because their Lua scripts don't support fetchNext — those return
// (nil, err).
func (w *Worker) finalise(jobID, token string, jobMap map[string]string, out handlerOutcome) (*prefetchedJob, finaliseStatus, error) {
	jobOpts := parseJobOpts(jobMap["opts"])

	if out.success {
		pf, err := w.finishCompleted(jobID, token, jobOpts, out.returnValue)
		if err != nil {
			return pf, finaliseStatusError, err
		}
		return pf, finaliseStatusCompleted, nil
	}

	// **遅延の要求は失敗ではない。** 試行回数 (atm) も retry の判断も通さず、
	// そのまま delayed へ戻す。BullMQ の DelayedError と同じく moveToDelayed の
	// skip-attempt を立てる。failedReason / stacktrace も書かない — 書くと
	// 管理画面が失敗した job に見せる。
	var delayed *DelayedError
	if errors.As(out.err, &delayed) {
		if err := w.moveToDelayedKeepingAttempts(jobID, token, delayed.Delay); err != nil {
			return nil, finaliseStatusError, err
		}
		return nil, finaliseStatusDelayed, nil
	}

	// Re-read atm from Redis instead of trusting the moveToActive
	// snapshot. BullMQ's current stalled-recovery doesn't bump atm
	// (only stc), so the snapshot would be correct today, but a
	// fresh HGET removes the dependency on that invariant and keeps
	// the retry decision robust if BullMQ semantics shift later.
	atm := w.fetchAtm(jobID, jobMap)

	// 失敗のたびに stacktrace を積み、その試行の開始時刻を残す。
	hist := buildFailureHistory(jobMap, out.errReason)

	if !w.shouldRetry(jobOpts, atm, out.err) {
		pf, err := w.finishFailed(jobID, token, jobOpts, out.errReason, hist)
		if err != nil {
			return pf, finaliseStatusError, err
		}
		return pf, finaliseStatusFailed, nil
	}

	delay := w.computeRetryDelay(jobOpts.backoff, BackoffContext{
		JobID:        jobID,
		Name:         jobMap["name"],
		AttemptsMade: atm + 1,
		Err:          out.err,
	})

	// **負の遅延は「もう再試行するな」。** BullMQ の settings.backoffStrategy は
	// -1 を返すことで残りの試行を打ち切る (job.ts:790 の
	// `delay == -1 ? false : true`)。TS から移してきた strategy がそのまま
	// 動くよう、符号の意味を合わせる。合わせないと「諦めろ」が「今すぐ再送しろ」
	// に反転し、残り試行を間髪入れずに焼き切ってしまう。
	if delay < 0 {
		pf, err := w.finishFailed(jobID, token, jobOpts, out.errReason, hist)
		if err != nil {
			return pf, finaliseStatusError, err
		}
		return pf, finaliseStatusFailed, nil
	}
	if delay > 0 {
		if err := w.retryDelayed(jobID, token, delay, out.errReason, hist); err != nil {
			return nil, finaliseStatusError, err
		}
		return nil, finaliseStatusRetrying, nil
	}
	if err := w.retryImmediate(jobID, token, jobOpts.lifo, out.errReason, hist); err != nil {
		return nil, finaliseStatusError, err
	}
	return nil, finaliseStatusRetrying, nil
}

// finishCompleted writes the BullMQ completed-state transition.
// jobOpts.removeOnComplete is forwarded into MoveToFinishedOpts.KeepJobs
// so the vendored Lua trims the completed ZSET per BullMQ semantics.
func (w *Worker) finishCompleted(jobID, token string, opts jobOpts, returnValue string) (*prefetchedJob, error) {
	return w.runMoveToFinished(jobID, token, opts.attempts, opts.removeOnComplete,
		"completed", "returnvalue", returnValue, nil)
}

// finishFailed writes the BullMQ failed-state transition with the
// failedReason + stacktrace HASH fields. attempts is forwarded so the
// vendored Lua only emits `retries-exhausted` when retries are
// genuinely exhausted; jobOpts.removeOnFail drives failed ZSET
// retention.
func (w *Worker) finishFailed(jobID, token string, opts jobOpts, reason string, hist failureHistory) (*prefetchedJob, error) {
	return w.runMoveToFinished(jobID, token, opts.attempts, opts.removeOnFail,
		"failed", "failedReason", reason,
		[]any{"stacktrace", hist.stacktrace, attemptHistoryField, hist.attemptsAt})
}

// runMoveToFinished is the shared moveToFinished invocation. extraFields
// piggybacks on ARGV[9] for the failed path to write stacktrace
// alongside the state change. keep is the per-target retention
// (nil = keep all, count==0 = remove immediately, count>0 = keep last
// n; age trims by seconds since timestamp).
//
// Passes ARGV[6]="1" (BullMQ's fetchNext sentinel) so the Lua's
// fetchNextJob helper opportunistically dequeues the next wait /
// prioritized job under the same lock token. When the wait list is
// non-empty the response is the dequeued job's HGETALL array (same
// shape moveToActive returns); when empty the response is the
// regular completion code. Caller consumes the prefetched job on
// the next dispatchLoop iteration without a moveToActive RTT —
// halving the dispatch-side EVALSHA load that caps consumer
// throughput vs BullMQ TS at high concurrency.
func (w *Worker) runMoveToFinished(jobID, token string, attempts int, keep *retentionLimit, target, msgProperty, msgValue string, extraFields []any) (*prefetchedJob, error) {
	now := time.Now().UnixMilli()

	optsArgs := proto.MoveToFinishedOpts{
		Token:          token,
		LockDuration:   w.cfg.lockDuration.Milliseconds(),
		Attempts:       attempts,
		Name:           w.cfg.workerName,
		MaxMetricsSize: w.cfg.maxDataPoints,
	}
	// fetchNext=1 の lua 経路 (fetchNextJob) は opts['limiter'] を読んで
	// rate limit gating する。worker config に limiter があればここで
	// 引き渡す — 無いと fetchNext が rate limit を bypass してテストが
	// 通らない (TestWorker_RateLimit_Throttles 等)。
	if l := w.cfg.limiter; l != nil {
		optsArgs.Limiter = &proto.MoveToActiveLimiter{
			Max:        l.max,
			DurationMs: l.duration.Milliseconds(),
		}
	}
	if keep != nil {
		kj := &proto.KeepJobs{Count: keep.count}
		if keep.ageSeconds != nil {
			kj.Age = *keep.ageSeconds
		}
		optsArgs.KeepJobs = kj
	}
	optsBytes, err := proto.EncodeMoveToFinishedOpts(optsArgs)
	if err != nil {
		return nil, fmt.Errorf("encode finish opts: %w", err)
	}

	// `finishedOn` を ARGV[9] に積まない: moveToFinished-14.lua が
	// 末尾で HSET timestamp 同値を書き込むため重複になる。空 / extra
	// だけを渡すことで余計な HMSET ペアを削減し、BullMQ TS の
	// updateData 設計と揃える。
	jobFields, err := proto.EncodeJobFields(extraFields...)
	if err != nil {
		return nil, fmt.Errorf("encode job fields: %w", err)
	}

	keys := w.keys.moveToFinishedKeys(jobID, target)
	res, err := w.scripts.Run(
		context.Background(),
		lua.MoveToFinished,
		keys,
		jobID,
		now,
		msgProperty,
		msgValue,
		target,
		w.fetchNextFlag(),
		w.keys.prefix,
		optsBytes,
		jobFields,
	)
	if err != nil {
		return nil, fmt.Errorf("moveToFinished(%s): %w", target, err)
	}
	// Polymorphic response: int64 = no fetched job (success / error code);
	// []any = fetchNext returned the next job's data (or a no-job branch).
	if code, ok := res.(int64); ok {
		if code < 0 {
			return nil, fmt.Errorf("moveToFinished(%s) returned error code %d", target, code)
		}
		return nil, nil
	}
	jobMap, prefetchedID, gotJob, _, _, perr := parseMoveToActiveResult(res)
	if perr != nil {
		return nil, fmt.Errorf("moveToFinished(%s) parse fetchNext: %w", target, perr)
	}
	if !gotJob {
		// fetchNextJob returned a no-job branch (rate-limited / paused /
		// idle). Treat as plain success — dispatcher's next iteration
		// will hit moveToActive normally and pick up whichever signal
		// applies.
		return nil, nil
	}
	return &prefetchedJob{jobMap: jobMap, jobID: prefetchedID, token: token}, nil
}

// fetchNextFlag returns the BullMQ ARGV[6] sentinel for moveToFinished:
// "1" enables the fetchNext optimisation, "" disables it. We must
// disable when runCtx is cancelled, otherwise the lua's fetchNextJob
// path would lock a fresh job that the worker is about to abandon
// (dispatchLoop's exit guard wouldn't process it). The locked job
// would then sit in active for `lockDuration + stalledInterval`
// before stalled-recovery reclaims it. Mirrors BullMQ TS's
// `!this.closing` gate at moveToFinished call sites.
//
// Race window: ctx can be cancelled between this check and the EVAL
// reaching Redis, in which case one job slips through and ends up
// abandoned. dispatchLoop's deferred cleanup releases the lock for
// any prefetched job the dispatcher holds at exit, narrowing that
// window to the in-flight EVAL plus a single dispatch hop.
func (w *Worker) fetchNextFlag() string {
	if w.runCtx.Err() != nil {
		return ""
	}
	return "1"
}

// releasePrefetchedLock drops the lock the lua acquired for the
// prefetched job before the dispatcher exited. Best-effort: if the
// release fails (Redis hiccup, lock already expired) the lock TTL
// will eventually clear it via stalled-recovery. The deferred call
// in dispatchLoop guarantees this runs even on panic.
func (w *Worker) releasePrefetchedLock(pj *prefetchedJob) {
	if pj == nil {
		return
	}
	// shutdown race の発生 (fetchNext で job を locked したまま停止) は
	// 通常 0 件のはずなので、一件でも観測されたら ops に伝えたい。
	// debug-level: 単発であれば運用上問題ないが、頻発するなら shutdown
	// 同期に問題があることを示すシグナル。
	w.logger.Debug("mkq: releasing prefetched lock at shutdown",
		slog.String(AttrQueue, w.queueName),
		slog.String(AttrJobID, pj.jobID),
	)
	_, _ = w.scripts.Run(
		context.Background(),
		lua.ReleaseLock,
		[]string{w.keys.jobLock(pj.jobID)},
		pj.token,
		w.cfg.lockDuration.Milliseconds(),
	)
}

// retryImmediate re-enqueues a failed job via retryJob-11.lua. The Lua
// bumps `atm` (HINCRBY) atomically and writes failedReason/stacktrace
// via ARGV[6] (jobFieldsToUpdate, msgpack flat array).
func (w *Worker) retryImmediate(jobID, token string, lifo bool, reason string, hist failureHistory) error {
	now := time.Now().UnixMilli()
	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}

	jobFields, err := proto.EncodeJobFields("failedReason", reason,
		"stacktrace", hist.stacktrace, attemptHistoryField, hist.attemptsAt)
	if err != nil {
		return fmt.Errorf("encode retry job fields: %w", err)
	}

	keys := w.keys.retryJobKeys(jobID)
	res, err := w.scripts.Run(
		context.Background(),
		lua.RetryJob,
		keys,
		w.keys.prefix,
		now,
		pushCmd,
		jobID,
		token,
		jobFields,
	)
	if err != nil {
		return fmt.Errorf("retryJob: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("retryJob returned error code %d", code)
	}
	return nil
}

// moveToDelayedKeepingAttempts moves an active job back to delayed for
// delay without bumping atm, for handlers that returned a DelayedError.
func (w *Worker) moveToDelayedKeepingAttempts(jobID, token string, delay time.Duration) error {
	// retryDelayed と揃えて 1 ms を床にする。0 以下でも Lua は過去の時刻で
	// delayed に入れてすぐ promote するので挙動は変わらないが、「delayed を
	// 通る」ことを値の上でも保証しておく。
	delayMs := max(delay.Milliseconds(), 1)
	keys := w.keys.moveToDelayedKeys(jobID)
	res, err := w.scripts.Run(
		context.Background(),
		lua.MoveToDelayed,
		keys,
		w.keys.prefix,
		time.Now().UnixMilli(),
		jobID,
		token,
		delayMs,
		"1", // skip attempt = true: the job did not fail, so atm stays
		"",  // no job fields to update
		"",  // fetchNext=false
		"",  // opts
	)
	if err != nil {
		return fmt.Errorf("moveToDelayed: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("moveToDelayed returned error code %d", code)
	}
	return nil
}

// retryDelayed re-enqueues with a delay via moveToDelayed-11.lua,
// honouring exponential / fixed backoff computed Go-side.
func (w *Worker) retryDelayed(jobID, token string, delay time.Duration, reason string, hist failureHistory) error {
	now := time.Now().UnixMilli()
	// 防御: computeBackoffDelay は >0 のはずだが、誤って 0 を渡すと
	// Lua が delayedTimestamp = timestamp + 0 で即時 promote 候補に
	// なる。1 ms 床値で「delayed」状態を確実にする。
	delayMs := max(delay.Milliseconds(), 1)

	jobFields, err := proto.EncodeJobFields("failedReason", reason,
		"stacktrace", hist.stacktrace, attemptHistoryField, hist.attemptsAt)
	if err != nil {
		return fmt.Errorf("encode retry job fields: %w", err)
	}

	keys := w.keys.moveToDelayedKeys(jobID)
	res, err := w.scripts.Run(
		context.Background(),
		lua.MoveToDelayed,
		keys,
		w.keys.prefix,
		now,
		jobID,
		token,
		delayMs,
		"0", // skip attempt = false: bump atm on this retry
		jobFields,
		"", // fetchNext=false
		"", // opts: BullMQ accepts empty for the basic retry case
	)
	if err != nil {
		return fmt.Errorf("moveToDelayed: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("moveToDelayed returned error code %d", code)
	}
	return nil
}

// computeRetryDelay resolves the per-attempt retry delay for a job,
// layering jitter and the custom strategy on top of the built-in
// fixed / exponential base computed by computeBackoffDelay.
//
// A RetryDelayFunc registered with WithRetryDelayOverride is consulted
// first, whatever the type and even when the job carries no backoff at
// all. Declining there falls through to everything below.
//
// For built-in types the un-jittered base is computed wire-faithfully
// and, when BackoffStrategy.Jitter > 0, spread via applyJitter using
// the BullMQ formula. For any non-built-in type ("custom") the
// Worker-registered strategy owns the entire computation (formula +
// cap + jitter): WithBackoffStrategyFunc first, then the older
// attempt-count-only WithBackoffStrategy. With neither registered the
// job falls back to immediate retry (delay 0) rather than panicking, so
// foreign custom-typed jobs never wedge the worker.
func (w *Worker) computeRetryDelay(b *BackoffStrategy, bc BackoffContext) time.Duration {
	if bc.AttemptsMade < 1 {
		return 0
	}
	if b != nil {
		bc.BackoffType = b.Type
	}

	// **型に関係なく先に相談する。** 「普段は exponential でよいが、相手が
	// Retry-After を返したときだけそれに従う」は backoff type を custom に
	// しないと書けなかった。custom にするとカーブ全体を呼び出し側が持つ
	// ことになるうえ、`opts.backoff.type` が Redis に載って BullMQ TS 側の
	// 挙動まで変わる。意見が無ければ下の分岐に委ねられる形にしてある。
	if w.retryDelayOverride != nil {
		if d, ok := w.callRetryDelayOverride(bc); ok {
			return d
		}
	}

	if b == nil {
		return 0
	}
	switch b.Type {
	case "fixed", "exponential":
		base := computeBackoffDelay(b, bc.AttemptsMade)
		if b.Jitter > 0 {
			return applyJitter(base, b.Jitter, w.rng())
		}
		return base
	default:
		return w.callBackoffStrategy(bc)
	}
}

// callBackoffStrategy invokes the user-registered strategy under panic
// recovery.
//
// **ここは handler の recover の外側。** runHandler の defer はすでに戻って
// いて、dispatchLoop 自身は recover を持たない。ユーザのコードが panic すると
// dispatch goroutine ごと落ち、プロセスが死んで、掴んでいたジョブは lock TTL が
// 切れるまで active に取り残される。文脈付きの strategy は errors.As と
// フィールドアクセスを誘う分だけ panic しやすいので、ここで止める。
func (w *Worker) callBackoffStrategy(bc BackoffContext) (d time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			if w.logger != nil {
				w.logger.Error("mkq: backoff strategy panicked; retrying immediately",
					slog.String(AttrQueue, w.queueName),
					slog.String(AttrJobID, bc.JobID),
					slog.Any("panic", r),
				)
			}
			// 名前付き戻り値はこの時点でゼロのままなので、strategy 未登録時と
			// 同じ「即時再試行」に落ちる。意図を明示するために代入しておく
			// (挙動としては冗長)。
			d = 0
		}
	}()
	if w.backoffFunc != nil {
		return w.backoffFunc(bc)
	}
	if w.backoffStrategy != nil {
		return w.backoffStrategy(bc.AttemptsMade)
	}
	return 0
}

// callRetryDelayOverride invokes the registered override under panic
// recovery, for the same reason callBackoffStrategy does: this runs
// after runHandler's recover has returned and the dispatch loop has
// none of its own.
//
// A panic is treated as "no opinion" rather than "retry immediately".
// Swallowing it into delay 0 would silently turn a considered backoff
// into a hot loop against whatever just failed.
func (w *Worker) callRetryDelayOverride(bc BackoffContext) (d time.Duration, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			if w.logger != nil {
				w.logger.Error("mkq: retry delay override panicked; falling back to the configured backoff",
					slog.String(AttrQueue, w.queueName),
					slog.String(AttrJobID, bc.JobID),
					slog.Any("panic", r),
				)
			}
			d, ok = 0, false
		}
	}()
	return w.retryDelayOverride(bc)
}

// jobOpts is the subset of the per-job opts JSON that the worker
// needs after dequeue. The on-Redis HASH `opts` field is the JSON
// shape produced by Job.optsAsJSON in BullMQ TS.
type jobOpts struct {
	attempts         int
	backoff          *BackoffStrategy
	lifo             bool
	removeOnComplete *retentionLimit
	removeOnFail     *retentionLimit
}

// retentionLimit captures the count + age the worker needs to forward
// into MoveToFinishedOpts.KeepJobs.
type retentionLimit struct {
	count      *int
	ageSeconds *int
}

// parseJobOpts deserialises the BullMQ HASH `opts` JSON into the
// fields the retry / retention paths need.
//
// BullMQ TypeScript stores several option fields polymorphically:
//
//   - removeOnComplete / removeOnFail accept `boolean | number |
//     {count, age}` and are persisted as-is.
//   - backoff accepts `number | {type, delay}` and is persisted
//     without normalisation (Backoffs.normalize runs at retry-time).
//
// Decoding these as concrete Go types would surface a
// json.UnmarshalTypeError on the first foreign-style entry and
// poison the entire opts struct, silently disabling retry for jobs
// added by other-language workers. Instead we capture each
// polymorphic field as json.RawMessage and convert per field below,
// so a typing surprise only loses that one option.
//
// Missing or malformed top-level JSON falls back to zero values
// (no retry / no backoff / FIFO push / keep all).
func parseJobOpts(raw string) jobOpts {
	var out jobOpts
	if raw == "" || raw == "{}" {
		return out
	}
	var m struct {
		Attempts         int             `json:"attempts"`
		Lifo             bool            `json:"lifo"`
		Backoff          json.RawMessage `json:"backoff"`
		RemoveOnComplete json.RawMessage `json:"removeOnComplete"`
		RemoveOnFail     json.RawMessage `json:"removeOnFail"`
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return out
	}
	out.attempts = m.Attempts
	out.lifo = m.Lifo
	out.backoff = parseBackoffOpt(m.Backoff)
	out.removeOnComplete = parseRemoveOpt(m.RemoveOnComplete)
	out.removeOnFail = parseRemoveOpt(m.RemoveOnFail)
	return out
}

// parseRemoveOpt converts a BullMQ removeOnComplete / removeOnFail
// raw JSON value into the {count, age} pair mkq forwards into Lua.
//
//	null / missing / false   -> nil  (BullMQ default: keep all)
//	true                     -> &{count: 0}  (remove on completion)
//	number N                 -> &{count: N}
//	{"count": C, "age": A}   -> &{count: C, age: A} (either field optional)
//
// Anything else is treated as "not understood" and returns nil.
func parseRemoveOpt(raw json.RawMessage) *retentionLimit {
	if len(raw) == 0 {
		return nil
	}
	s := string(raw)
	switch s {
	case "null", "false":
		return nil
	case "true":
		v := 0
		return &retentionLimit{count: &v}
	}
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return &retentionLimit{count: &n}
	}
	var obj struct {
		Count *int `json:"count"`
		Age   *int `json:"age"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil && (obj.Count != nil || obj.Age != nil) {
		return &retentionLimit{count: obj.Count, ageSeconds: obj.Age}
	}
	return nil
}

// parseBackoffOpt accepts BullMQ's `number | {type, delay, jitter}`
// shape:
//
//	number N                   -> &BackoffStrategy{Type:"fixed", Delay: N ms}
//	{type, delay, jitter?}     -> &BackoffStrategy{Type, Delay, Jitter}
//
// jitter is optional (defaults to 0). Empty / null / unrecognised
// shapes return nil so callers fall back to the no-backoff default.
func parseBackoffOpt(raw json.RawMessage) *BackoffStrategy {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var ms int64
	if err := json.Unmarshal(raw, &ms); err == nil {
		return &BackoffStrategy{
			Type:  "fixed",
			Delay: time.Duration(ms) * time.Millisecond,
		}
	}
	var obj struct {
		Type   string  `json:"type"`
		Delay  int64   `json:"delay"`
		Jitter float64 `json:"jitter"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil && obj.Type != "" {
		return &BackoffStrategy{
			Type:   obj.Type,
			Delay:  time.Duration(obj.Delay) * time.Millisecond,
			Jitter: obj.Jitter,
		}
	}
	return nil
}

// shouldRetry encodes BullMQ's worker-side retry decision. attemptsMade
// is the count BEFORE this failure (the value stored in HASH `atm`);
// the +1 mirrors BullMQ's `attemptsMade + 1 < opts.attempts` check.
func (w *Worker) shouldRetry(o jobOpts, attemptsMade int, err error) bool {
	if errors.Is(err, ErrUnrecoverable) {
		return false
	}
	if o.attempts <= 0 {
		return false
	}
	return attemptsMade+1 < o.attempts
}

// fetchAtm reads the most recent attempts-made counter from Redis,
// falling back to the moveToActive snapshot if the HGET fails. This
// closes the small race window where another worker / stalled
// recovery could have advanced atm between dequeue and finalise.
func (w *Worker) fetchAtm(jobID string, jobMap map[string]string) int {
	// Use a short-lived ctx independent of runCtx so that worker
	// shutdown doesn't cause us to under-count and bypass retries.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := w.rdb.HGet(ctx, w.keys.job(jobID), "atm").Result()
	if err != nil {
		return parseInt(jobMap["atm"])
	}
	return parseInt(v)
}

func parseInt(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// invokeHandler dispatches to the typed Handler[T] via the closure
// captured at Process time. We pay one type assertion per job to keep
// Worker / Queue independent of T.
func invokeHandler(ctx context.Context, handlerAny any, jobID string, jobMap map[string]string) (any, error) {
	invoke, ok := handlerAny.(func(ctx context.Context, jobID string, jobMap map[string]string) (any, error))
	if !ok {
		return nil, fmt.Errorf("mkq: handler shim has unexpected type %T", handlerAny)
	}
	return invoke(ctx, jobID, jobMap)
}

// newHandlerShim wraps a typed Handler[T] in the type-erased adapter
// the Worker invokes. Lives here so worker.go owns the only place that
// reaches into Job[T] reconstruction. The Queue[T] back-pointer is
// stitched into every reconstructed Job[T] so handler-side mutation
// methods (Job.UpdateProgress / Job.UpdateData / Job.Log) work
// without forcing handlers to capture the queue separately.
func newHandlerShim[T any](q *Queue[T], h Handler[T]) any {
	return func(ctx context.Context, jobID string, jobMap map[string]string) (any, error) {
		job, err := buildJob[T](jobID, jobMap)
		if err != nil {
			return nil, fmt.Errorf("mkq: rebuild job %s: %w", jobID, err)
		}
		job.queue = q
		return h(ctx, job)
	}
}

func buildJob[T any](jobID string, jobMap map[string]string) (*Job[T], error) {
	var data T
	if raw, ok := jobMap["data"]; ok && raw != "" {
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
	}
	tsMs, _ := strconv.ParseInt(jobMap["timestamp"], 10, 64)
	return &Job[T]{
		ID:              jobID,
		Name:            jobMap["name"],
		Data:            data,
		Timestamp:       time.UnixMilli(tsMs),
		AttemptsMade:    parseInt(jobMap["atm"]),
		AttemptsStarted: parseInt(jobMap["ats"]),
		StalledCounter:  parseInt(jobMap["stc"]),
		ProcessedBy:     jobMap["pb"],
	}, nil
}

// parseMoveToActiveResult interprets the polymorphic EVALSHA response.
// Lua returns one of:
//   - {HGETALL_array, jobId, 0, 0} on success
//   - {0, 0, expireTime, 0} when rate-limited
//   - {0, 0, 0, nextDelayedTs} when waiting on a delayed job
//   - {0, 0, 0, 0} when paused / maxed / empty
//
// expireTimeMs surfaces the rate-limit cooldown so dispatchLoop can
// sleep precisely. nextDelayedTs surfaces the next delayed job's
// absolute target ms timestamp so dispatchLoop can sleep until it
// (BZPopMin's 1s floor would otherwise overshoot sub-1s delays).
func parseMoveToActiveResult(res any) (jobMap map[string]string, jobID string, ok bool, expireTimeMs int64, nextDelayedTs int64, err error) {
	arr, isArr := res.([]any)
	if !isArr || len(arr) < 4 {
		return nil, "", false, 0, 0, nil
	}
	jobData, isJobData := arr[0].([]any)
	if !isJobData {
		// arr[0] が integer (=0) のとき = job 無し。Slot 2 (Go index)
		// は rate-limit の expireTime ms、Slot 3 は次 delayed job の
		// 絶対時刻 (ms)。dispatchLoop はこれを使って precise sleep する。
		expireTimeMs, _ = toInt64(arr[2])
		nextDelayedTs, _ = toInt64(arr[3])
		return nil, "", false, expireTimeMs, nextDelayedTs, nil
	}
	id, idOK := arr[1].(string)
	if !idOK || id == "" {
		return nil, "", false, 0, 0, nil
	}
	m, err := flatHashToMap(jobData)
	if err != nil {
		return nil, "", false, 0, 0, err
	}
	return m, id, true, 0, 0, nil
}

// toInt64 normalises the integer types go-redis can surface for an
// EVALSHA result element (int64 / int / int32 / float64) to int64.
// In practice go-redis v9 returns int64 for Lua integer returns;
// the other cases stay covered for forward compatibility.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

// flatHashToMap turns Redis HGETALL output `[k1, v1, k2, v2, ...]` into
// a map. go-redis surfaces strings here.
func flatHashToMap(arr []any) (map[string]string, error) {
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("HGETALL flat array has odd length %d", len(arr))
	}
	m := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		k, kok := arr[i].(string)
		v, vok := arr[i+1].(string)
		if !kok || !vok {
			return nil, fmt.Errorf("HGETALL element %d/%d not a string", i, i+1)
		}
		m[k] = v
	}
	return m, nil
}

// mustJSONString JSON-encodes v; on failure (which would only happen
// for genuinely non-marshalable values) it falls back to a string so
// the failed/completed transition still surfaces *something*.
func mustJSONString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		fallback, _ := json.Marshal(fmt.Sprintf("mkq: failed to marshal value: %v", err))
		return string(fallback)
	}
	return string(b)
}

// defaultWorkerName matches BullMQ's "host:pid:uuid" convention loosely.
func defaultWorkerName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
}

// newQueueKeys snapshots the per-queue Redis key set used by all four
// worker scripts.
func newQueueKeys[T any](q *Queue[T]) queueKeys {
	b := q.keys
	return queueKeys{
		wait:             b.Wait(),
		active:           b.Active(),
		prioritized:      b.Prioritized(),
		events:           b.Events(),
		stalled:          b.Stalled(),
		limiter:          b.Limiter(),
		delayed:          b.Delayed(),
		paused:           b.Paused(),
		meta:             b.Meta(),
		pc:               b.PriorityCounter(),
		marker:           b.Marker(),
		completed:        b.Completed(),
		failed:           b.Failed(),
		metricsCompleted: b.Metrics("completed"),
		metricsFailed:    b.Metrics("failed"),
		stalledCheck:     b.StalledCheck(),
		prefix:           b.Base(),
		repeat:           b.Repeat(),
		id:               b.ID(),
		builder:          b,
	}
}

func (k queueKeys) jobLock(jobID string) string {
	return k.prefix + jobID + ":lock"
}

func (k queueKeys) job(jobID string) string {
	return k.prefix + jobID
}

// moveToActiveKeys assembles KEYS[1..11] for moveToActive-11.lua.
func (k queueKeys) moveToActiveKeys() []string {
	return []string{
		k.wait, k.active, k.prioritized, k.events, k.stalled,
		k.limiter, k.delayed, k.paused, k.meta, k.pc,
		k.marker,
	}
}

// moveToFinishedKeys assembles KEYS[1..14] for moveToFinished-14.lua.
// KEYS[11] / KEYS[13] toggle between completed and failed depending
// on target. KEYS[13] is always populated with the BullMQ-spec
// metrics key; the Lua's outer `if maxMetricsSize ~= ""` guards the
// actual write so passing the real key when the feature is disabled
// is harmless.
func (k queueKeys) moveToFinishedKeys(jobID, target string) []string {
	resultSet := k.completed
	metricsKey := k.metricsCompleted
	if target == "failed" {
		resultSet = k.failed
		metricsKey = k.metricsFailed
	}
	return []string{
		k.wait, k.active, k.prioritized, k.events, k.stalled,
		k.limiter, k.delayed, k.paused, k.meta, k.pc,
		resultSet, k.job(jobID), metricsKey, k.marker,
	}
}

// retryJobKeys assembles KEYS[1..11] for retryJob-11.lua.
//
//	KEYS[1]  active     KEYS[2]  wait        KEYS[3]  paused
//	KEYS[4]  job key    KEYS[5]  meta        KEYS[6]  events
//	KEYS[7]  delayed    KEYS[8]  prioritized KEYS[9]  pc
//	KEYS[10] marker     KEYS[11] stalled
func (k queueKeys) retryJobKeys(jobID string) []string {
	return []string{
		k.active, k.wait, k.paused, k.job(jobID), k.meta, k.events,
		k.delayed, k.prioritized, k.pc, k.marker, k.stalled,
	}
}

// moveStalledKeys assembles KEYS[1..9] for moveStalledJobsToWait-9.lua.
//
//	KEYS[1] stalled SET   KEYS[2] wait LIST    KEYS[3] active LIST
//	KEYS[4] stalled-check KEYS[5] meta         KEYS[6] paused
//	KEYS[7] marker        KEYS[8] events stream KEYS[9] repeat
func (k queueKeys) moveStalledKeys() []string {
	// BullMQ 6 で KEYS[9] に repeat key が加わった。stalled 回収時に
	// scheduler 由来のジョブを見分けるため。paused (KEYS[6]) は残っている。
	return []string{
		k.stalled, k.wait, k.active, k.stalledCheck, k.meta,
		k.paused, k.marker, k.events, k.repeat,
	}
}

// moveToDelayedKeys assembles KEYS[1..11] for moveToDelayed-11.lua.
//
//	KEYS[1]  marker     KEYS[2]  active      KEYS[3]  prioritized
//	KEYS[4]  delayed    KEYS[5]  job key     KEYS[6]  events
//	KEYS[7]  meta       KEYS[8]  stalled     KEYS[9]  wait
//	KEYS[10] limiter    KEYS[11] pc
func (k queueKeys) moveToDelayedKeys(jobID string) []string {
	// BullMQ 6 で paused key が KEYS から外れた (pause 中でもジョブは wait に
	// 入るので、行き先を分ける必要が無くなったため)。順序は
	// moveToDelayed-11.lua のヘッダどおり。
	return []string{
		k.marker, k.active, k.prioritized, k.delayed, k.job(jobID),
		k.events, k.meta, k.stalled, k.wait, k.limiter, k.pc,
	}
}

// attemptHistoryField is the HASH field holding the start time (unix
// milliseconds) of every failed attempt, as a JSON array of numbers.
//
// **BullMQ には対応物が無い。** BullMQ TS も試行ごとの時刻を残さないので、
// admin UI は再試行を並べようとしても置く時刻が無い (upstream Misskey の
// job 詳細が試行を `at ?` と表示しているのはこれが理由)。mkq の拡張として
// 記録する。未知 field は BullMQ / bull-board が無視するので wire 互換は保たれる。
const attemptHistoryField = "mkqAttemptsAt"

// failureHistory accumulates the per-failure fields that BullMQ keeps as
// arrays. jobMap is the moveToActive snapshot, so the previous values are
// already in hand and no extra round trip is needed.
type failureHistory struct {
	stacktrace string // JSON array of strings
	attemptsAt string // JSON array of unix-ms numbers
}

// buildFailureHistory appends this failure to whatever the job already
// carried.
//
// **上書きせず積む。** BullMQ TS は stacktrace を試行ごとに append する。
// mkq は 1 要素で上書きしていたので、**再試行した job の過去の失敗理由が
// 残らなかった**。積む側に揃える。
//
// attemptsAt にはその試行の開始時刻 (processedOn) を積む。失敗時刻ではなく
// 開始時刻なのは、admin UI が「何回目がいつ走ったか」を並べるため。
func buildFailureHistory(jobMap map[string]string, reason string) failureHistory {
	traces := decodeJSONStrings(jobMap["stacktrace"])
	traces = append(traces, reason)

	starts := decodeJSONInt64s(jobMap[attemptHistoryField])
	if v, err := strconv.ParseInt(jobMap["processedOn"], 10, 64); err == nil && v > 0 {
		starts = append(starts, v)
	}
	return failureHistory{
		stacktrace: mustJSONString(traces),
		attemptsAt: mustJSONString(starts),
	}
}

// decodeJSONStrings best-effort decodes a JSON string array. 壊れていたら
// 捨てる — 記録の継続より、今回の失敗を残すことを優先する。
func decodeJSONStrings(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

// decodeJSONInt64s best-effort decodes a JSON number array.
func decodeJSONInt64s(raw string) []int64 {
	if raw == "" {
		return nil
	}
	var out []int64
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}
