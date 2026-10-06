package mkq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/elythia-network/mkq/internal/lua"
)

// Config configures a Client.
type Config struct {
	// Redis is forwarded to redis.NewUniversalClient. Addrs at minimum
	// must be set.
	Redis redis.UniversalOptions
	// KeyPrefix maps to BullMQ's "keyPrefix" option. Empty defaults to
	// "bull" to match BullMQ's own default.
	KeyPrefix string

	// Logger receives operational records (stalled-scan failures,
	// NOSCRIPT reload events, etc.). Nil = noop. Adapters live under
	// github.com/elythia-network/mkq/observability/... — opt-in.
	Logger Logger
	// Metrics receives counter / histogram / gauge updates from the
	// hot paths. Nil = noop. See the Metrics interface godoc for the
	// signal set mkq emits.
	Metrics Metrics
	// Tracer creates spans around Queue.Add and handler invocation.
	// Nil = noop. See the Tracer interface godoc for span names.
	Tracer Tracer
}

// Client is a long-lived handle to a Redis-backed mkq deployment. A
// Client owns the underlying redis.UniversalClient and the cached Lua
// SHAs; queues constructed via Define share both.
type Client struct {
	rdb       redis.UniversalClient
	keyPrefix string
	scripts   *lua.Scripter
	logger    Logger
	metrics   Metrics
	tracer    Tracer
	// configuredPoolSize は cfg.Redis.PoolSize の保存。0 は go-redis の
	// 既定値 (10*GOMAXPROCS) を意味する。Worker 起動時の pool-size
	// 警告に使う。
	configuredPoolSize int
}

// NewClient connects to Redis, preloads the vendored BullMQ Lua scripts,
// and returns a ready-to-use Client. Callers must Close the returned
// Client to release the Redis connection pool.
//
// The context bounds connection setup (PING + SCRIPT LOAD); cancel it
// to abort a hung connect attempt.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Redis.Addrs) == 0 {
		return nil, errors.New("mkq: Config.Redis.Addrs is required")
	}
	rdb := redis.NewUniversalClient(&cfg.Redis)

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("mkq: redis ping: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = noopLogger{}
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = noopMetrics{}
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = noopTracer{}
	}

	scripts, err := lua.NewScripter(ctx, rdb, logger)
	if err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("mkq: load scripts: %w", err)
	}

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = defaultKeyPrefix
	}

	return &Client{
		rdb:                rdb,
		keyPrefix:          prefix,
		configuredPoolSize: cfg.Redis.PoolSize,
		scripts:            scripts,
		logger:             logger,
		metrics:            metrics,
		tracer:             tracer,
	}, nil
}

// Close releases the underlying Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// defaultKeyPrefix matches BullMQ's default. Mirrored here rather than
// imported from internal/keys to keep the public surface free of
// internal-package leaks.
const defaultKeyPrefix = "bull"

// queueRegistryKey is the auxiliary SET that tracks every queue
// Define[T] has touched against this Client. The key uses a double
// colon (empty queue-name slot) so it cannot collide with any
// BullMQ queue key — BullMQ requires a non-empty queue name, so
// `{prefix}::queues` is unreachable from the BullMQ wire format.
//
// BullMQ itself has no queue registry (bull-board accepts the queue
// list as configuration); this is an mkq-only addition. Foreign
// queues that BullMQ TS created without mkq Define'ing them won't
// appear here — the registry under-reports rather than over-reports.
func (c *Client) queueRegistryKey() string {
	return c.keyPrefix + "::queues"
}

// Queues returns every queue name mkq has Define'd against this
// client (idempotent registration in Define keeps the SET clean).
//
// Foreign queues not Define'd through mkq are absent; for a complete
// view, mk-go-style admin paths can layer a SCAN-based discovery on
// top — that's a separate API addition tracked in the inspector
// follow-up.
func (c *Client) Queues(ctx context.Context) ([]string, error) {
	res, err := c.rdb.SMembers(ctx, c.queueRegistryKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("mkq: SMembers %s: %w", c.queueRegistryKey(), err)
	}
	return res, nil
}

// DiscoverOption tunes DiscoverQueues.
type DiscoverOption func(*discoverConfig)

type discoverConfig struct {
	count int64
}

// WithDiscoverCount sets the SCAN COUNT hint. Larger values finish in
// fewer round-trips at the cost of longer individual SCAN calls. Zero
// or negative leaves the default.
func WithDiscoverCount(n int) DiscoverOption {
	return func(c *discoverConfig) {
		if n > 0 {
			c.count = int64(n)
		}
	}
}

// defaultDiscoverCount is the SCAN COUNT hint. 100 keeps each call
// short enough not to disturb a busy Redis while still finishing a
// few-thousand-key keyspace in a handful of round-trips.
const defaultDiscoverCount = 100

// DiscoverQueues enumerates the queues present in Redis under this
// client's key prefix, including ones mkq never Define'd — a queue
// created by a BullMQ worker in another language, or by another
// process.
//
// Queues reports only what this client Define'd, which is the right
// answer for "what does this process work on" and the wrong one for
// "what is in this deployment". DiscoverQueues answers the second.
//
// **SCAN は安い操作ではないので admin 用途に限る。** 配送のたびに呼ぶような
// ものではなく、ダッシュボードや CLI が人の操作に応じて撃つことを想定して
// いる。
//
// A queue that mkq Define'd shows up here even with no jobs ever
// enqueued, because Define writes the queue's meta key. It is not a
// strict superset of Queues, though: the registry SET is only ever
// added to, so a queue whose keys were obliterated or evicted keeps its
// entry there while disappearing from here.
func (c *Client) DiscoverQueues(ctx context.Context, opts ...DiscoverOption) ([]string, error) {
	cfg := discoverConfig{count: defaultDiscoverCount}
	for _, o := range opts {
		o(&cfg)
	}

	// BullMQ が必ず作るキューレベルのキーは meta。言語を問わず拾える。
	// prefix はユーザ入力なので、glob のメタ文字を含んでいても文字どおりに
	// 照合されるようエスケープする。
	//
	// このパターンの狭さは正しさの担保ではなく、走査を安く保つためのもの。
	// 紛れ込んだものを弾くのは keepQueueMeta の役目。
	keys, err := c.scanAll(ctx, escapeGlob(c.keyPrefix)+":*:meta", cfg.count)
	if err != nil {
		return nil, err
	}
	keys, err = c.keepQueueMeta(ctx, keys)
	if err != nil {
		return nil, err
	}

	found := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if name, ok := c.queueNameFromMetaKey(k); ok {
			found[name] = struct{}{}
		}
	}

	out := make([]string, 0, len(found))
	for name := range found {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// queueNameFromMetaKey peels "{prefix}:" off the front and ":meta" off
// the back.
//
// **キュー名に ":" が入っていてもこれで正しく戻る。** 前後が固定なので
// 曖昧さは無い。ただし prefix 自体が別の prefix の接頭辞になっている場合
// (例: "bull" と "bull:app") は、外側の SCAN が内側のキーも拾ってしまう。
// これは BullMQ のキー設計そのものの性質で、mkq 側では区別できない。
func (c *Client) queueNameFromMetaKey(key string) (string, bool) {
	const suffix = ":meta"
	prefix := c.keyPrefix + ":"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", false
	}
	name := key[len(prefix) : len(key)-len(suffix)]
	if name == "" {
		return "", false
	}
	return name, true
}

// metaMarkerFields are fields only a queue's meta hash carries:
// "version" is written by mkq's Define, "opts.maxLenEvents" by any job
// add (BullMQ's getOrSetMaxEvents), "paused" by pause. Job hashes and
// job-scheduler hashes have none of them.
var metaMarkerFields = []string{"version", "opts.maxLenEvents", "paused"}

// keepQueueMeta narrows SCAN hits down to actual queue metadata.
//
// **":meta" で終わるキーは queue meta とは限らない。** ジョブ ID も
// scheduler ID も dedup ID も任意の文字列を取れるので、
// `{prefix}:{queue}:{jobId}` / `:repeat:{id}` / `:de:{id}` のどれもが
// パターンに引っかかりうる。しかも dedup キーは STRING なので、HASH 前提の
// コマンドを撃つと WRONGTYPE でパイプライン全体が落ち、キューが 1 本も
// 見えなくなる。
//
// そこで「job hash ではないもの」という否定ではなく、「meta だけが持つ
// フィールドがあるもの」という肯定で判定する。型が違うキーは WRONGTYPE を
// 読み飛ばすだけで済み、SCAN と本判定の間に消えたキーも (全フィールドが
// nil になるので) 安全側に落ちる。
func (c *Client) keepQueueMeta(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.HMGet(ctx, k, metaMarkerFields...)
	}
	if _, err := pipe.Exec(ctx); err != nil && !isWrongType(err) {
		return nil, fmt.Errorf("mkq: discover queues: %w", err)
	}

	out := keys[:0]
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			if isWrongType(err) {
				continue
			}
			return nil, fmt.Errorf("mkq: discover queues: %w", err)
		}
		for _, v := range cmd.Val() {
			if v != nil {
				out = append(out, keys[i])
				break
			}
		}
	}
	return out, nil
}

// isWrongType reports whether err is Redis' "you used a hash command on
// something that is not a hash". go-redis surfaces it as a plain server
// error with no sentinel to compare against.
func isWrongType(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE")
}

// escapeGlob quotes the characters Redis' MATCH reads as pattern syntax.
//
// **key prefix は設定由来なので glob のメタ文字が入りうる。** 例えば
// "bull[prod]" をそのまま埋めると `[prod]` が文字クラスになり、実在する
// キーに一生当たらないまま「キューが 0 本」という静かな嘘を返す。
func escapeGlob(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// scanAll runs SCAN to completion, across every master when the client
// is a cluster one (SCAN is per-node, so a single cursor would only
// ever see one shard).
func (c *Client) scanAll(ctx context.Context, pattern string, count int64) ([]string, error) {
	if cluster, ok := c.rdb.(*redis.ClusterClient); ok {
		var (
			mu  sync.Mutex
			out []string
		)
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			keys, err := scanNode(ctx, node, pattern, count)
			if err != nil {
				return err
			}
			mu.Lock()
			out = append(out, keys...)
			mu.Unlock()
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("mkq: scan %s: %w", pattern, err)
		}
		return out, nil
	}
	keys, err := scanNode(ctx, c.rdb, pattern, count)
	if err != nil {
		return nil, fmt.Errorf("mkq: scan %s: %w", pattern, err)
	}
	return keys, nil
}

func scanNode(ctx context.Context, rdb redis.Cmdable, pattern string, count int64) ([]string, error) {
	var (
		out    []string
		cursor uint64
	)
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}
