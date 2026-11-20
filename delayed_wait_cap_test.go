package mkq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorker_DelayedWaitCap pins #150: with a far delayed job pending, the
// marker wait lasts the longer of maxIdleWait and IdlePollInterval.
func TestWorker_DelayedWaitCap(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		// window is how long nothing may be sent once the dispatcher parks.
		window time.Duration
	}{
		// 待機の間隔が 30 秒より長いときは、その長さまで待つ。30 秒で切ると、
		// 窓の中で取り出しと marker 待ちを撃ち直す。
		{"longer interval", maxIdleWait + 5*time.Second, maxIdleWait + time.Second},
		// 短いときは 30 秒まで待つ。待機の間隔で切ると、窓の中で撃ち直す。
		{"shorter interval", 2 * time.Second, 6 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prefix := fmt.Sprintf("mkqtest-%s-%d", t.Name(), time.Now().UnixNano())
			c := internalTestClient(t, prefix)

			type payload struct {
				N int `json:"n"`
			}
			queue := Define[payload](c, "deliver")
			_, err := queue.Add(context.Background(), payload{}, WithDelay(time.Hour))
			require.NoError(t, err)

			hook := &countingHook{}
			c.rdb.AddHook(hook)
			worker, err := Process(queue, func(context.Context, *Job[payload]) (any, error) {
				return nil, nil
			}, WithConcurrency(1), WithStalledInterval(0), WithIdlePollInterval(tc.interval))
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := worker.Stop(ctx); err != nil {
					t.Logf("stop: %v", err)
				}
			})

			// 積んだときの marker を取り、取り出しの後にもう一度 marker 待ちに
			// 入ったところから数える。marker 待ちの中にいて、前に見たときから
			// 1 件も撃っていなければ、入り直した後とみなす。
			last := int64(-1)
			require.Eventually(t, func() bool {
				worker.markerMu.Lock()
				parked := worker.markerWaiters == 1
				worker.markerMu.Unlock()
				n := hook.n.Load()
				settled := parked && n == last
				last = n
				return settled
			}, 10*time.Second, 300*time.Millisecond)
			before := hook.n.Load()
			time.Sleep(tc.window)
			assert.Zero(t, hook.n.Load()-before,
				"the marker wait with a far delayed job ended before max(maxIdleWait, IdlePollInterval)")
		})
	}
}
