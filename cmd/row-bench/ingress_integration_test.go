//go:build integration

package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
)

func TestOfficialIngressRollsBackFactsWhenEnqueueFails(t *testing.T) {
	t.Chdir("../..")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e, err := lab.New(ctx, "control")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	i, err := startIngress(ctx, e.URL, "pgboss", "benchmarks/pgboss", filepath.Join(t.TempDir(), "ingress"))
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	const sent int64 = 1789823456789012345 // Deliberately above JavaScript's exact Number range.
	if err := i.Write(ctx, e.DB, []int64{1}, []int64{sent}, []string{"first"}); err != nil {
		t.Fatal(err)
	}
	var actual int64
	if err := e.DB.QueryRow(ctx, "SELECT sent_ns FROM expected WHERE id=1").Scan(&actual); err != nil || actual != sent {
		t.Fatalf("timestamp precision: got %d, error %v", actual, err)
	}
	// Reject the SDK's job INSERT, after business rows, facts and oracle have been
	// inserted in the same transaction. Existing committed jobs remain untouched.
	if _, err := e.DB.Exec(ctx, "ALTER TABLE pgboss.job ADD CONSTRAINT reject_fixture CHECK (name <> 'events') NOT VALID"); err != nil {
		t.Fatal(err)
	}
	if err := i.Write(ctx, e.DB, []int64{2}, []int64{sent + 1}, []string{"rollback"}); err == nil {
		t.Fatal("SDK enqueue rejection was reported as a successful commit")
	}
	for _, table := range []string{"items", "expected", "bench_fact", "pgboss.job"} {
		var count int
		if err := e.DB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s: count=%d, error=%v", table, count, err)
		}
	}
}
