package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLongActiveDrainDoesNotReplayAndStalledDrainExpires(t *testing.T) {
	ctx, activity, finish := idleContext(context.Background(), 200*time.Millisecond)
	defer finish()
	for range 10 {
		select {
		case <-ctx.Done():
			t.Fatal("active snapshot exceeded the old whole-step timeout")
		case <-time.After(40 * time.Millisecond):
			activity()
		}
	}
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatal("wrong inactivity failure")
		}
	case <-time.After(time.Second):
		t.Fatal("stalled source operation was not canceled")
	}
	activity()
	if ctx.Err() == nil {
		t.Fatal("progress resurrected an expired operation")
	}
	parent, cancel := context.WithCancel(context.Background())
	child, touch, closeChild := idleContext(parent, time.Hour)
	defer closeChild()
	cancel()
	touch()
	if !errors.Is(child.Err(), context.Canceled) {
		t.Fatal("activity overrode shutdown")
	}
}
