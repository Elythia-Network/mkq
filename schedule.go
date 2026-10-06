package mkq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/elythia-network/mkq/internal/lua"
	"github.com/elythia-network/mkq/internal/proto"
)

// scheduleConfig is the typed-option bag for UpsertScheduleEvery /
// UpsertSchedulePattern. Public callers populate it via WithSchedule*
// options; internal re-upserts (worker auto-schedule) construct it
// directly.
type scheduleConfig struct {
	limit       int
	endDate     time.Time
	startDate   time.Time
	tz          string
	immediately bool

	// retention は「いつ発火するか」ではなく「各 iteration の job に
	// どんな opts が載るか」の話なので、scheduleOpts ではなく template
	// 側に入る。
	template proto.ScheduleTemplateOpts
}

// ScheduleOption customises an UpsertScheduleEvery / UpsertSchedulePattern
// call. Some options (timezone, immediately) only make sense for
// pattern-mode and are validated at upsert time.
type ScheduleOption func(*scheduleConfig)

// WithScheduleLimit caps the total number of fires for a schedule.
// Once reached, no further instances are created. n <= 0 disables
// the cap (BullMQ default).
func WithScheduleLimit(n int) ScheduleOption {
	return func(c *scheduleConfig) { c.limit = n }
}

// WithScheduleEndDate sets the absolute time after which no new
// instances are scheduled. A zero time disables the bound.
func WithScheduleEndDate(t time.Time) ScheduleOption {
	return func(c *scheduleConfig) { c.endDate = t }
}

// WithScheduleStartDate sets the earliest time at which the first
// instance may fire. A zero time means "fire as soon as possible
// after upsert".
func WithScheduleStartDate(t time.Time) ScheduleOption {
	return func(c *scheduleConfig) { c.startDate = t }
}

// WithScheduleTimezone sets the IANA timezone (e.g. "Asia/Tokyo")
// in which a cron pattern is evaluated. Pattern-mode only; passing
// it to UpsertScheduleEvery returns an error. Empty string = local
// time, matching BullMQ's default when `tz` is omitted.
func WithScheduleTimezone(tz string) ScheduleOption {
	return func(c *scheduleConfig) { c.tz = tz }
}

// WithScheduleImmediately makes the first iteration fire at upsert
// time rather than waiting for the next pattern match. Pattern-mode
// only. Mutually exclusive with WithScheduleStartDate (BullMQ TS
// rejects the combination at upsertJobScheduler).
func WithScheduleImmediately() ScheduleOption {
	return func(c *scheduleConfig) { c.immediately = true }
}

// WithScheduleKeepCompleted bounds the completed ZSET for the jobs a
// schedule creates, mirroring WithKeepCompleted on Add. n==0 removes
// each iteration as soon as it completes; n>0 keeps the most recent n.
//
// **これを付けないと定期ジョブの completed は無期限に溜まる。** 週次の
// 保守ジョブでも年単位で回れば数万件になり、job HASH もその数だけ残る。
// BullMQ TS 側も同じ理由で template の opts に retention を置いている。
func WithScheduleKeepCompleted(n int) ScheduleOption {
	return func(c *scheduleConfig) {
		c.template.RemoveOnComplete = mergeRetention(c.template.RemoveOnComplete, &n, nil)
	}
}

// WithScheduleKeepCompletedAge drops completed iterations older than
// age. Combine with WithScheduleKeepCompleted to bound by both.
// BullMQ stores age at one-second resolution.
func WithScheduleKeepCompletedAge(age time.Duration) ScheduleOption {
	return func(c *scheduleConfig) {
		secs := int(age.Seconds())
		c.template.RemoveOnComplete = mergeRetention(c.template.RemoveOnComplete, nil, &secs)
	}
}

// WithScheduleKeepFailed is the failed-side analogue of
// WithScheduleKeepCompleted.
func WithScheduleKeepFailed(n int) ScheduleOption {
	return func(c *scheduleConfig) {
		c.template.RemoveOnFail = mergeRetention(c.template.RemoveOnFail, &n, nil)
	}
}

// WithScheduleKeepFailedAge is the failed-side analogue of
// WithScheduleKeepCompletedAge.
func WithScheduleKeepFailedAge(age time.Duration) ScheduleOption {
	return func(c *scheduleConfig) {
		secs := int(age.Seconds())
		c.template.RemoveOnFail = mergeRetention(c.template.RemoveOnFail, nil, &secs)
	}
}

// mergeRetention lets the count and age options be combined in either
// order without one clearing the other.
func mergeRetention(cur *proto.RetentionLimit, count, ageSeconds *int) *proto.RetentionLimit {
	out := &proto.RetentionLimit{}
	if cur != nil {
		out.Count, out.AgeSeconds = cur.Count, cur.AgeSeconds
	}
	if count != nil {
		out.Count = count
	}
	if ageSeconds != nil {
		out.AgeSeconds = ageSeconds
	}
	return out
}

// UpsertScheduleEvery registers (or replaces) a fixed-interval
// recurring schedule for q. The first instance is queued immediately
// (subject to WithScheduleStartDate); subsequent instances are
// queued by the worker after each fire completes.
//
// scheduleID identifies the schedule across iterations; calling
// UpsertScheduleEvery again with the same id replaces the schedule
// in place (override-true semantics).
//
// payload is JSON-encoded into the BullMQ template `data` field and
// reused for every instance.
func (q *Queue[T]) UpsertScheduleEvery(
	ctx context.Context,
	scheduleID string,
	every time.Duration,
	payload T,
	opts ...ScheduleOption,
) error {
	if scheduleID == "" {
		return fmt.Errorf("mkq: scheduleID must be non-empty")
	}
	if every <= 0 {
		return fmt.Errorf("mkq: every must be positive")
	}

	cfg := scheduleConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.tz != "" || cfg.immediately {
		return fmt.Errorf("mkq: WithScheduleTimezone / WithScheduleImmediately are pattern-mode only; use UpsertSchedulePattern")
	}

	dataJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("mkq: marshal schedule payload: %w", err)
	}

	scheduleOpts := proto.ScheduleOpts{
		Name:    q.name,
		EveryMs: every.Milliseconds(),
		Limit:   cfg.limit,
	}
	if !cfg.startDate.IsZero() {
		scheduleOpts.StartDate = cfg.startDate.UnixMilli()
	}
	if !cfg.endDate.IsZero() {
		scheduleOpts.EndDate = cfg.endDate.UnixMilli()
	}

	// every-mode の初回 fire は max(startDate, now)。lua の
	// getJobSchedulerEveryNextMillis と同じ式を Go 側で再現し、
	// firstFire > endDate なら dead schedule を作らずに reject。
	// pattern-mode 側 (UpsertSchedulePattern) と同じ gating ポリシー。
	if !cfg.endDate.IsZero() {
		firstFire := time.Now()
		if !cfg.startDate.IsZero() && cfg.startDate.After(firstFire) {
			firstFire = cfg.startDate
		}
		if firstFire.After(cfg.endDate) {
			return fmt.Errorf("mkq: first fire %v exceeds endDate %v; schedule would never run", firstFire, cfg.endDate)
		}
	}

	// every-mode は ARGV[1]="0" → lua が getJobSchedulerEveryNextMillis
	// で次 millis を再計算する。
	return q.upsertSchedule(ctx, scheduleID, "0", scheduleOpts, cfg.template, string(dataJSON))
}

// UpsertSchedulePattern registers (or replaces) a cron-pattern
// recurring schedule. Behaves like UpsertScheduleEvery except next
// fire times are derived from the cron expression Go-side (see
// cron.go for the supported subset, which mirrors BullMQ's
// cron-parser@4.9.0 mainstream syntax).
//
// pattern is a 5-field cron expression or a standard descriptor
// macro (`@daily`, etc.). `@every <duration>` is rejected — use
// UpsertScheduleEvery for fixed intervals.
//
// WithScheduleTimezone(tz) controls the timezone for evaluation
// (default: local). WithScheduleImmediately() makes the first
// iteration fire at upsert time; mutually exclusive with
// WithScheduleStartDate per BullMQ TS validation.
func (q *Queue[T]) UpsertSchedulePattern(
	ctx context.Context,
	scheduleID string,
	pattern string,
	payload T,
	opts ...ScheduleOption,
) error {
	if scheduleID == "" {
		return fmt.Errorf("mkq: scheduleID must be non-empty")
	}

	cfg := scheduleConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.immediately && !cfg.startDate.IsZero() {
		return fmt.Errorf("mkq: WithScheduleImmediately and WithScheduleStartDate are mutually exclusive")
	}

	cc, err := parseCron(pattern, cfg.tz)
	if err != nil {
		return err
	}

	dataJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("mkq: marshal schedule payload: %w", err)
	}

	scheduleOpts := proto.ScheduleOpts{
		Name:    q.name,
		Pattern: pattern,
		TZ:      cfg.tz,
		Limit:   cfg.limit,
	}
	if !cfg.startDate.IsZero() {
		scheduleOpts.StartDate = cfg.startDate.UnixMilli()
	}
	if !cfg.endDate.IsZero() {
		scheduleOpts.EndDate = cfg.endDate.UnixMilli()
	}

	// pattern-mode は Go 側で次 millis を計算して ARGV[1] に渡す。
	// immediately なら今すぐ、startDate があればそれ以降の最初の
	// pattern マッチ、それ以外は now 以降の最初のマッチ。
	now := time.Now()
	var firstFire time.Time
	switch {
	case cfg.immediately:
		firstFire = now
	case !cfg.startDate.IsZero():
		firstFire = cc.nextFire(cfg.startDate)
	default:
		firstFire = cc.nextFire(now)
	}
	// endDate を超えた firstFire を許すと、yearly cron + endDate=24h の
	// ように lua 側で月単位の delay job を作ってしまう。worker の
	// rescheduleNext が以後の iteration を gate するのと同じロジックを
	// 初回 upsert にも適用し、dead schedule の作成を未然に弾く。
	if !cfg.endDate.IsZero() && firstFire.After(cfg.endDate) {
		return fmt.Errorf("mkq: first fire %v exceeds endDate %v; schedule would never run", firstFire, cfg.endDate)
	}
	nextMillis := strconv.FormatInt(firstFire.UnixMilli(), 10)

	return q.upsertSchedule(ctx, scheduleID, nextMillis, scheduleOpts, cfg.template, string(dataJSON))
}

// upsertSchedule is the wire-level call shared by UpsertScheduleEvery,
// UpsertSchedulePattern, and the worker's auto-rescheduling path.
// `nextMillis` is "0" for every-mode (lua recomputes) or the
// pre-computed pattern next-fire ms timestamp as a decimal string.
func (q *Queue[T]) upsertSchedule(
	ctx context.Context,
	scheduleID string,
	nextMillis string,
	scheduleOpts proto.ScheduleOpts,
	template proto.ScheduleTemplateOpts,
	dataJSON string,
) error {
	scheduleOptsBytes, err := proto.EncodeScheduleOpts(scheduleOpts)
	if err != nil {
		return fmt.Errorf("mkq: encode schedule opts: %w", err)
	}
	templateOptsBytes, err := proto.EncodeScheduleTemplateOpts(template)
	if err != nil {
		return fmt.Errorf("mkq: encode template opts: %w", err)
	}
	// 各 iteration の job HASH に書かれる opts。BullMQ TS Worker は
	// 自前で再スケジュールするとき opts.repeat.every / pattern を
	// 参照するので、foreign worker と互換にするため repeat ブロックを
	// 必ず埋める。
	delayedOptsBytes, err := proto.EncodeScheduleDelayedOpts(scheduleOpts, template)
	if err != nil {
		return fmt.Errorf("mkq: encode delayed opts: %w", err)
	}

	now := time.Now().UnixMilli()
	res, err := q.client.scripts.Run(
		ctx,
		lua.AddJobScheduler,
		q.scheduleKeys(),
		nextMillis,
		scheduleOptsBytes,
		scheduleID,
		dataJSON,
		templateOptsBytes,
		delayedOptsBytes,
		now,
		q.keys.Base(),
		"", // ARGV[9] producer key (optional)
	)
	if err != nil {
		return fmt.Errorf("mkq: addJobScheduler: %w", err)
	}
	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("mkq: addJobScheduler returned error code %d", code)
	}
	return nil
}

// RemoveSchedule deletes a schedule. The currently-queued delayed
// instance (if any) is also dropped, but already-running instances
// are not interrupted.
func (q *Queue[T]) RemoveSchedule(ctx context.Context, scheduleID string) error {
	if scheduleID == "" {
		return fmt.Errorf("mkq: scheduleID must be non-empty")
	}

	scheduleKey := q.keys.Schedule(scheduleID)
	// Drop the schedule template HASH and the repeat ZSET entry.
	// We don't reach into delayed/active for the in-flight instance
	// — those finalise normally and the worker's re-upsert path
	// short-circuits because the schedule template HASH is gone.
	pipe := q.client.rdb.Pipeline()
	pipe.Del(ctx, scheduleKey)
	pipe.ZRem(ctx, q.keys.Repeat(), scheduleID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("mkq: remove schedule %s: %w", scheduleID, err)
	}
	return nil
}

// scheduleKeys assembles KEYS[1..11] for addJobScheduler-11.lua.
//
//	KEYS[1]  repeat       KEYS[2]  delayed   KEYS[3]  wait
//	KEYS[4]  paused       KEYS[5]  meta      KEYS[6]  prioritized
//	KEYS[7]  marker       KEYS[8]  id        KEYS[9]  events
//	KEYS[10] pc           KEYS[11] active
func (q *Queue[T]) scheduleKeys() []string {
	return []string{
		q.keys.Repeat(),
		q.keys.Delayed(),
		q.keys.Wait(),
		q.keys.Paused(),
		q.keys.Meta(),
		q.keys.Prioritized(),
		q.keys.Marker(),
		q.keys.ID(),
		q.keys.Events(),
		q.keys.PriorityCounter(),
		q.keys.Active(),
	}
}
