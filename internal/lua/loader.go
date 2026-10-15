package lua

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Logger is the minimal logging surface lua.Scripter uses for
// NOSCRIPT-reload diagnostics. Decoupled from mkq.Logger to avoid
// an internal->root import cycle; mkq.Logger satisfies this
// interface implicitly because mkq.Logger.Debug shares the same
// (string, ...slog.Attr) signature.
type Logger interface {
	Debug(msg string, attrs ...slog.Attr)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...slog.Attr) {}

// ScriptName identifies a vendored entry-point script.
type ScriptName string

// Vendored entry-point scripts. The numeric suffix mirrors BullMQ's
// convention of encoding the expected KEYS count in the filename.
const (
	AddStandardJob        ScriptName = "addStandardJob-9"
	AddDelayedJob         ScriptName = "addDelayedJob-6"
	AddPrioritizedJob     ScriptName = "addPrioritizedJob-9"
	MoveToActive          ScriptName = "moveToActive-11"
	MoveToFinished        ScriptName = "moveToFinished-14"
	ExtendLock            ScriptName = "extendLock-2"
	ReleaseLock           ScriptName = "releaseLock-1"
	RetryJob              ScriptName = "retryJob-11"
	MoveToDelayed         ScriptName = "moveToDelayed-11"
	MoveStalledJobsToWait ScriptName = "moveStalledJobsToWait-9"
	AddJobScheduler       ScriptName = "addJobScheduler-11"
	UpdateJobScheduler    ScriptName = "updateJobScheduler-12"
	UpdateProgress        ScriptName = "updateProgress-3"
	UpdateData            ScriptName = "updateData-1"
	AddLog                ScriptName = "addLog-2"
	GetCounts             ScriptName = "getCounts-1"
	GetRanges             ScriptName = "getRanges-1"
	GetMetrics            ScriptName = "getMetrics-2"
	RemoveJob             ScriptName = "removeJob-2"
	Drain                 ScriptName = "drain-5"
	Promote               ScriptName = "promote-9"
	ReprocessJob          ScriptName = "reprocessJob-9"
	Pause                 ScriptName = "pause-7"
)

// Scripter executes vendored Lua scripts via EVALSHA, with transparent
// re-loading on NOSCRIPT.
//
// A Scripter is safe for concurrent use.
type Scripter struct {
	client redis.Scripter
	logger Logger

	mu      sync.RWMutex
	scripts map[ScriptName]*loadedScript

	// loadGroup deduplicates concurrent SCRIPT LOAD calls for the
	// same script name. Without it, N goroutines that simultaneously
	// observe NOSCRIPT after a Redis flush would each issue their
	// own SCRIPT LOAD round-trip; singleflight collapses them into
	// one upload, with the rest waiting on the shared result.
	loadGroup singleflight.Group
}

type loadedScript struct {
	source string
	sha    string
}

// NewScripter resolves and pre-loads the vendored entry-point scripts
// against the given Redis client. The returned Scripter caches each
// script's SHA so subsequent calls round-trip a single EVALSHA.
//
// logger may be nil; in that case a noop is used. Only NOSCRIPT-reload
// events are currently logged (Debug level).
func NewScripter(ctx context.Context, client redis.Scripter, logger Logger) (*Scripter, error) {
	if logger == nil {
		logger = noopLogger{}
	}
	s := &Scripter{
		client:  client,
		logger:  logger,
		scripts: make(map[ScriptName]*loadedScript),
	}
	for _, name := range []ScriptName{
		AddStandardJob, AddDelayedJob, AddPrioritizedJob,
		MoveToActive, MoveToFinished, ExtendLock, ReleaseLock,
		RetryJob, MoveToDelayed, MoveStalledJobsToWait,
		AddJobScheduler, UpdateJobScheduler,
		UpdateProgress, UpdateData, AddLog,
		GetCounts, GetRanges, GetMetrics,
		RemoveJob, Drain, Promote, ReprocessJob, Pause,
	} {
		if _, err := s.load(ctx, name); err != nil {
			return nil, fmt.Errorf("preload %s: %w", name, err)
		}
	}
	return s, nil
}

// Run executes the named script. On NOSCRIPT (e.g. after a Redis
// SCRIPT FLUSH or restart) the script is re-uploaded and the call is
// retried exactly once; further failures are returned to the caller.
func (s *Scripter) Run(ctx context.Context, name ScriptName, keys []string, args ...any) (any, error) {
	ls, err := s.get(name)
	if err != nil {
		return nil, err
	}
	res, err := s.client.EvalSha(ctx, ls.sha, keys, args...).Result()
	if err == nil {
		return res, nil
	}
	if !isNoScript(err) {
		return nil, err
	}
	// NOSCRIPT: Redisがflushされた等。再ロードして1度だけretry。
	// 通常は発生しないが、Redis flush / restart 後の最初のRunで観測
	// される。Ops が flush の発生を検知できるよう Debug log を残す。
	s.logger.Debug("mkq: lua script reload after NOSCRIPT",
		slog.String("script", string(name)),
	)
	ls, err = s.load(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("reload %s after NOSCRIPT: %w", name, err)
	}
	return s.client.EvalSha(ctx, ls.sha, keys, args...).Result()
}

func (s *Scripter) get(name ScriptName) (*loadedScript, error) {
	s.mu.RLock()
	ls, ok := s.scripts[name]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("script %q not loaded", name)
	}
	return ls, nil
}

func (s *Scripter) load(ctx context.Context, name ScriptName) (*loadedScript, error) {
	// singleflight collapses concurrent reloads for the same script
	// into a single SCRIPT LOAD round-trip; competing goroutines wait
	// on the shared result. The key is the script name so distinct
	// scripts can still load in parallel.
	v, err, _ := s.loadGroup.Do(string(name), func() (any, error) {
		src, err := Preprocess(string(name))
		if err != nil {
			return nil, err
		}
		sha, err := s.client.ScriptLoad(ctx, src).Result()
		if err != nil {
			return nil, fmt.Errorf("script load %s: %w", name, err)
		}
		ls := &loadedScript{source: src, sha: sha}
		s.mu.Lock()
		s.scripts[name] = ls
		s.mu.Unlock()
		return ls, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*loadedScript), nil
}

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	// go-redis v9 surfaces server-side errors via the redis.Error
	// interface; the string form preserves the original "NOSCRIPT ..."
	// prefix from Redis.
	var rerr redis.Error
	if errors.As(err, &rerr) {
		return strings.HasPrefix(rerr.Error(), "NOSCRIPT")
	}
	return strings.Contains(err.Error(), "NOSCRIPT")
}
