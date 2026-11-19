package mkq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingHook counts the commands a client sends.
type countingHook struct{ n atomic.Int64 }

func (h *countingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmd)
	}
}

func (h *countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.n.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// TestWorker_NonPositiveIdlePollIntervalUsesDefault pins #163: an idle
// worker given a zero or negative IdlePollInterval waits on the marker like
// the default instead of retrying the dequeue without a pause.
func TestWorker_NonPositiveIdlePollIntervalUsesDefault(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			t.Parallel()
			prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
			c := internalTestClient(t, prefix)
			hook := &countingHook{}
			c.rdb.AddHook(hook)
			// 起動の直後から数える。負の値は倍にしていくうちに桁あふれで正に
			// なり、それまでの数十回だけ空回りするので、起動の後に待ってから
			// 数えると見えない。
			before := hook.n.Load()

			type payload struct {
				N int `json:"n"`
			}
			worker, err := Process(Define[payload](c, "deliver"), func(context.Context, *Job[payload]) (any, error) {
				return nil, nil
			}, WithConcurrency(1), WithStalledInterval(0), WithIdlePollInterval(d))
			require.NoError(t, err)
			assert.Equal(t, defaultIdlePollInterval, worker.cfg.idlePollInterval)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := worker.Stop(ctx); err != nil {
					t.Logf("stop: %v", err)
				}
			})

			time.Sleep(3 * time.Second)
			// 既定なら marker 待ちは 1 秒に切り上がるので、3 秒で数回 (1 回に取り出しと
			// BZPOPMIN の 2 件)。0 なら数千回、負の値でも数十回の空回りがある。
			assert.Less(t, hook.n.Load()-before, int64(15))
		})
	}
}
