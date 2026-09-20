//go:build integration

package relay_test

import (
	"testing"
	"time"

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
