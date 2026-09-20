package relay

import (
	"context"
	"time"
)

// A snapshot has no row-count bound. Its active drain may exceed one operation
// timeout; only a stalled read/publish/ACK should force whole-snapshot replay.
func idleContext(parent context.Context, timeout time.Duration) (context.Context, func(), func()) {
	ctx, cancel := context.WithCancelCause(parent)
	timer := time.AfterFunc(timeout, func() { cancel(context.DeadlineExceeded) })
	return ctx, func() { timer.Reset(timeout) }, func() {
		timer.Stop()
		cancel(context.Canceled)
	}
}
