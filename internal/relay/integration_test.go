//go:build integration

package relay_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fixture(t *testing.T, mode string) (context.Context, *lab.Env, *relay.Runner, *kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	e, err := lab.New(ctx, mode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	cfg := e.Config()
	cfg.Batch = 2 // Deliberately much smaller than a source snapshot batch.
	r, err := relay.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	c, err := e.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return ctx, e, r, c
}

func tick(t *testing.T, ctx context.Context, e *lab.Env, mode string) {
	t.Helper()
	if mode == "pgque" {
		if _, err := e.DB.Exec(ctx, "SELECT pgque.ticker('rowrelay')"); err != nil {
			t.Fatal(err)
		}
	}
}

func drain(t *testing.T, ctx context.Context, e *lab.Env, r *relay.Runner, c *kgo.Client, mode string, want int) [][]byte {
	t.Helper()
	var values [][]byte
	for len(values) < want && ctx.Err() == nil {
		tick(t, ctx, e, mode)
		if _, _, err := r.Step(ctx); err != nil {
			t.Fatal(err)
		}
		poll, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		fetches := c.PollRecords(poll, want-len(values))
		cancel()
		for _, fetchErr := range fetches.Errors() {
			if !errors.Is(fetchErr.Err, context.DeadlineExceeded) && !errors.Is(fetchErr.Err, context.Canceled) {
				t.Fatal(fetchErr)
			}
		}
		fetches.EachRecord(func(record *kgo.Record) { values = append(values, append([]byte(nil), record.Value...)) })
	}
	if len(values) != want {
		t.Fatalf("received %d, want %d: %v", len(values), want, ctx.Err())
	}
	return values
}

func payload(t *testing.T, value []byte) (string, string) {
	t.Helper()
	var envelope struct {
		Headers map[string]string `json:"headers"`
		Payload string            `json:"payload"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Headers["ID"], envelope.Payload
}

func TestCaptureAndSnapshotLargerThanChunk(t *testing.T) {
	for _, mode := range []string{"pgque"} {
		t.Run(mode, func(t *testing.T) {
			ctx, e, r, c := fixture(t, mode)
			_, err := e.DB.Exec(ctx, `INSERT INTO items SELECT i, 1, 'payload', NULL FROM generate_series(1,17) i`)
			if err != nil {
				t.Fatal(err)
			}
			values := drain(t, ctx, e, r, c, mode, 17)
			seen := map[string]bool{}
			for _, value := range values {
				id, body := payload(t, value)
				if seen[id] || !strings.Contains(body, `"optional": null`) {
					t.Fatalf("duplicate or missing SQL NULL: %s", value)
				}
				seen[id] = true
			}
			var super, replication bool
			if err = e.DB.QueryRow(ctx, "SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname=current_user").Scan(&super, &replication); err != nil || super || replication {
				t.Fatalf("privileged application role: %v", err)
			}
			if _, err = e.DB.Exec(ctx, "TRUNCATE items"); err == nil {
				t.Fatal("TRUNCATE bypassed capture")
			}
		})
	}
}

func TestLateCommitRollbackSavepointAndImages(t *testing.T) {
	for _, mode := range []string{"pgque"} {
		t.Run(mode, func(t *testing.T) {
			ctx, e, r, c := fixture(t, mode)
			other, err := pgx.Connect(ctx, e.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close(ctx)
			tx, _ := other.Begin(ctx)
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, "INSERT INTO items VALUES (1,1,'late',NULL)"); err != nil {
				t.Fatal(err)
			}
			if _, err = e.DB.Exec(ctx, "INSERT INTO items VALUES (2,1,'early',NULL)"); err != nil {
				t.Fatal(err)
			}
			values := drain(t, ctx, e, r, c, mode, 1)
			_, body := payload(t, values[0])
			if !strings.Contains(body, "early") {
				t.Fatal("uncommitted event escaped")
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			values = drain(t, ctx, e, r, c, mode, 1)
			_, body = payload(t, values[0])
			if !strings.Contains(body, "late") {
				t.Fatal("lower ID late commit skipped")
			}
			_, err = e.DB.Exec(ctx, `BEGIN;
				SAVEPOINT s; INSERT INTO items VALUES(99,1,'rolled back',NULL); ROLLBACK TO s;
				UPDATE items SET id=3, optional='new-key' WHERE id=1;
				DELETE FROM items WHERE id=3; COMMIT;
				BEGIN; INSERT INTO items VALUES(100,1,'rolled back',NULL); ROLLBACK;`)
			if err != nil {
				t.Fatal(err)
			}
			values = drain(t, ctx, e, r, c, mode, 2)
			for i, value := range values {
				_, body = payload(t, value)
				want := []string{"UPDATE", "DELETE"}[i]
				if !strings.Contains(body, want) || !strings.Contains(body, "new-key") || strings.Contains(body, "rolled back") {
					t.Fatalf("bad old/new image or phantom: %s", body)
				}
			}
			tick(t, ctx, e, mode)
			if n, _, err := r.Step(ctx); n != 0 || err != nil {
				t.Fatalf("unexpected events after rollback: %d %v", n, err)
			}
			poll, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			if n := c.PollRecords(poll, 10).NumRecords(); n != 0 {
				t.Fatalf("extra events after rollback: %d", n)
			}
		})
	}
}

func TestKafkaAckBeforeSourceAckAndReplay(t *testing.T) {
	for _, mode := range []string{"pgque"} {
		t.Run(mode, func(t *testing.T) {
			ctx, e, r, c := fixture(t, mode)
			if _, err := e.DB.Exec(ctx, "INSERT INTO items VALUES(1,1,'durable',NULL)"); err != nil {
				t.Fatal(err)
			}
			table, condition := "pgque.subscription", "NEW.sub_batch IS NULL AND OLD.sub_batch IS NOT NULL"
			_, err := e.Admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION public.fail_ack() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'injected lost source ack'; END $$;
				CREATE TRIGGER fail_ack BEFORE UPDATE ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION public.fail_ack()`, table, condition))
			if err != nil {
				t.Fatal(err)
			}
			tick(t, ctx, e, mode)
			if _, _, err = r.Step(ctx); err == nil {
				t.Fatal("injected source ACK failure did not fail")
			}
			r.Close() // Actual new publisher connection/process generation on replay.
			if _, err = e.Admin.Exec(ctx, "DROP TRIGGER fail_ack ON "+table); err != nil {
				t.Fatal(err)
			}
			r, err = relay.Open(ctx, e.Config())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(r.Close)
			values := drain(t, ctx, e, r, c, mode, 2)
			first, _ := payload(t, values[0])
			second, _ := payload(t, values[1])
			if first != second {
				t.Fatal("replay changed event identity")
			}
		})
	}
}

func TestConcurrentPublisherRejected(t *testing.T) {
	ctx, e, _, _ := fixture(t, "pgque")
	second, err := relay.Open(ctx, e.Config())
	if err == nil {
		second.Close()
		t.Fatal("second publisher acquired active source")
	}
}

func TestEmptySnapshotBatchStillAdvances(t *testing.T) {
	ctx, e, r, _ := fixture(t, "pgque")
	if _, err := e.DB.Exec(ctx, "SELECT pgque.force_next_tick('rowrelay')"); err != nil {
		t.Fatal(err)
	}
	tick(t, ctx, e, "pgque")
	if n, advanced, err := r.Step(ctx); n != 0 || !advanced || err != nil {
		t.Fatalf("empty completed batch confused with idle source: %d %v %v", n, advanced, err)
	}
	if n, advanced, err := r.Step(ctx); n != 0 || advanced || err != nil {
		t.Fatalf("idle source reported progress: %d %v %v", n, advanced, err)
	}
}

func TestPoisonNeverAcknowledgesLaterRecords(t *testing.T) {
	for _, mode := range []string{"pgque"} {
		t.Run(mode, func(t *testing.T) {
			ctx, e, r, c := fixture(t, mode)
			if _, err := e.DB.Exec(ctx, "INSERT INTO items VALUES(1,1,'good',NULL)"); err != nil {
				t.Fatal(err)
			}
			sql := "SELECT pgque.insert_event('rowrelay','row','secret-invalid-json')"
			if _, err := e.DB.Exec(ctx, sql); err != nil {
				t.Fatal(err)
			}
			if _, err := e.DB.Exec(ctx, "INSERT INTO items VALUES(2,1,'later',NULL)"); err != nil {
				t.Fatal(err)
			}
			tick(t, ctx, e, mode)
			for range 2 {
				if _, _, err := r.Step(ctx); err == nil || strings.Contains(err.Error(), "secret") {
					t.Fatalf("poison skipped or leaked: %v", err)
				}
			}
			poll, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			if n := c.PollRecords(poll, 10).NumRecords(); n != 0 {
				t.Fatalf("poison batch was published: %d", n)
			}
		})
	}
}

func TestCaptureRejectsOversizeAtomically(t *testing.T) {
	ctx, e, r, _ := fixture(t, "pgque")
	if _, err := e.DB.Exec(ctx, "INSERT INTO items VALUES(1,1,repeat('x',524289),NULL)"); err == nil {
		t.Fatal("oversized image accepted")
	}
	var count int
	if err := e.DB.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&count); err != nil || count != 0 {
		t.Fatalf("business write survived failed capture: %d %v", count, err)
	}
	if n, _, err := r.Step(ctx); n != 0 || err != nil {
		t.Fatalf("failed capture left an event: %d %v", n, err)
	}
}

func TestBrokerRejectionDoesNotAcknowledgeSource(t *testing.T) {
	ctx, e, first, _ := fixture(t, "pgque")
	first.Close()
	name := e.Name + "_blocked"
	maxBytes := "128"
	admin := kadm.NewClient(e.Kafka)
	if _, err := admin.CreateTopic(ctx, 1, 1, map[string]*string{"max.message.bytes": &maxBytes}, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.DeleteTopics(context.Background(), name) })
	cfg := e.Config()
	cfg.Topic, cfg.Timeout = name, 2*time.Second
	cfg.Compression = "none"
	r, err := relay.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	if _, err = e.DB.Exec(ctx, "INSERT INTO items VALUES(1,1,repeat('x',4000),NULL)"); err != nil {
		t.Fatal(err)
	}
	stepCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tick(t, ctx, e, "pgque")
	if _, _, err = r.Step(stepCtx); err == nil {
		t.Fatal("broker accepted a record exceeding its configured byte limit")
	}
	var unacknowledged bool
	if err = e.DB.QueryRow(ctx, "SELECT sub_batch IS NOT NULL FROM pgque.subscription").Scan(&unacknowledged); err != nil || !unacknowledged {
		t.Fatalf("source acknowledged without Kafka ACK: unacknowledged=%v error=%v", unacknowledged, err)
	}
}
