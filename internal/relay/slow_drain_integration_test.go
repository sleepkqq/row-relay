//go:build integration

package relay_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/health"
	"github.com/sleepkqq/row-relay/internal/relay"
)

func TestActiveSnapshotOutlivesOperationAndOwnershipIdleTimeouts(t *testing.T) {
	ctx, e, initial, _ := fixture(t, "pgque")
	initial.Close()
	c := e.Config()
	c.DeliveryMode, c.ManagedInvalidation = "managed", true
	c.ProgressInterval, c.Timeout, c.Batch = time.Second, 2*time.Second, 1
	r, err := relay.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = relay.DelayProducerForTest(r, 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err = e.DB.Exec(ctx, "INSERT INTO items SELECT n,1,'slow bounded delivery',NULL FROM generate_series(1,25) n"); err != nil {
		t.Fatal(err)
	}
	tick(t, ctx, e, "pgque")
	start := time.Now()
	count, advanced, err := r.Step(ctx)
	if err != nil || count != 25 || !advanced {
		t.Fatalf("drain: count=%d advanced=%v err=%v", count, advanced, err)
	}
	if time.Since(start) <= c.Timeout+c.Poll+5*time.Second {
		t.Fatal("test did not outlive the source ownership idle timeout")
	}
	var complete bool
	if err = e.DB.QueryRow(ctx, "SELECT NOT EXISTS (SELECT FROM pgque.subscription WHERE sub_batch IS NOT NULL)").Scan(&complete); err != nil || !complete {
		t.Fatalf("active long drain was not acknowledged: complete=%v error=%v", complete, err)
	}
}

// TestHealthActivityKeepsLongDrainReadyAndAckGatesCompletion proves the observed
// activity hook, not a timer, keeps a healthy long drain from being reported
// stalled: after one successful step makes the worker active, a drain that
// outlives the health deadline stays ready because real operations report
// activity. A failed source acknowledgement gates the completed callback, and no
// activity is reported once RunObservedActivity returns.
func TestHealthActivityKeepsLongDrainReadyAndAckGatesCompletion(t *testing.T) {
	ctx, e, initial, _ := fixture(t, "pgque")
	initial.Close()
	c := e.Config()
	c.DeliveryMode, c.ManagedInvalidation = "managed", true
	c.ProgressInterval, c.Timeout, c.Batch = time.Second, 2*time.Second, 1
	r, err := relay.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = relay.DelayProducerForTest(r, 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	deadline := c.Timeout + c.Poll + time.Second
	h := health.New(map[string]time.Duration{"source": deadline})
	ready := func() bool {
		rec := httptest.NewRecorder()
		h.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/ready", nil))
		return rec.Code == 200
	}

	// Perform and drain prior successful source steps (idle reads) so the next
	// source tick is exactly the long-drain batch, then record the healthy state
	// the completed callback would report for that last step.
	for {
		_, advanced, err := r.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !advanced {
			break
		}
	}
	h.Success("source")

	// Pre-create the single slow batch, then fail the next source
	// acknowledgement. pgque.ticker emits empty ticks, and next_batch selects the
	// earliest tick without skipping them, so draining ticks first and creating
	// the batch before the run starts keeps the fault scoped to this batch rather
	// than a ticker's empty batch ahead of it.
	if _, err = e.DB.Exec(ctx, "INSERT INTO items SELECT n,1,'slow bounded delivery',NULL FROM generate_series(2,26) n"); err != nil {
		t.Fatal(err)
	}
	tick(t, ctx, e, "pgque")
	table, condition := "pgque.subscription", "NEW.sub_batch IS NULL AND OLD.sub_batch IS NOT NULL"
	if _, err = e.Admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION public.fail_ack() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected lost source ack'; END $$;
		CREATE TRIGGER fail_ack BEFORE UPDATE ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION public.fail_ack()`, table, condition)); err != nil {
		t.Fatal(err)
	}

	var completed, activity atomic.Int64
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- r.RunObservedActivity(runCtx,
			func() { completed.Add(1); h.Success("source") },
			func() { activity.Add(1); h.Activity("source") })
	}()

	// Wait for the long drain to start and report real activity. The whole test
	// shares the fixture's 45s context; the waits below (10s + 20s) stay well
	// inside that budget, and an exhausted context fails with its own message
	// instead of being reported as a stalled drain.
	if !waitForCondition(t, ctx, 10*time.Second, func() bool { return activity.Load() > 0 }) {
		t.Fatal("the long drain never reported activity")
	}
	base := completed.Load()

	// The long drain outlives the health deadline while the source
	// acknowledgement is still pending; it must stay ready throughout.
	drain := time.Now()
	sustained := false
	var runErr error
waiting:
	for {
		select {
		case runErr = <-done:
			break waiting
		default:
		}
		if ctx.Err() != nil {
			t.Fatal("fixture context budget exhausted before the long drain returned")
		}
		if completed.Load() != base {
			t.Fatal("failed source acknowledgement still completed a step")
		}
		if time.Since(drain) > deadline+500*time.Millisecond {
			sustained = true
			if !ready() {
				t.Fatal("health stalled during an active long drain")
			}
		}
		if time.Since(drain) > 20*time.Second {
			t.Fatal("long drain did not return within the test budget")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sustained {
		t.Fatalf("drain did not outlive the health deadline: err=%v elapsed=%s completed=%d activity=%d base=%d",
			runErr, time.Since(drain), completed.Load(), activity.Load(), base)
	}
	if runErr == nil {
		t.Fatal("a failed source acknowledgement did not fail the run")
	}
	if completed.Load() != base {
		t.Fatalf("completed callback ran without a source ACK: %d -> %d", base, completed.Load())
	}
	// Activity is reported only while RunObservedActivity is running.
	stopped := activity.Load()
	quiet := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(quiet) {
		if activity.Load() != stopped {
			t.Fatal("activity callback ran after RunObservedActivity returned")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForCondition(t *testing.T, ctx context.Context, limit time.Duration, ok func() bool) bool {
	t.Helper()
	expire := time.After(limit)
	for {
		if ok() {
			return true
		}
		select {
		case <-ctx.Done():
			return ok()
		case <-expire:
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}
