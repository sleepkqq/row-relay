//go:build integration

package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBusinessOutboxAtomicWireAndLateCommit(t *testing.T) {
	for _, mode := range []string{"pgque"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			e, err := lab.New(ctx, "outbox-"+mode)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			cfg := e.Config()
			cfg.OutboxStream, cfg.Batch = "events", 2
			wrong := cfg
			wrong.Topic = "not-registered"
			if unexpected, err := relay.Open(ctx, wrong); err == nil {
				unexpected.Close()
				t.Fatal("unregistered route accepted")
			}
			r, err := relay.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			c, err := e.Consumer()
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if second, err := relay.Open(ctx, cfg); err == nil {
				second.Close()
				t.Fatal("second owner accepted")
			}
			conn, err := pgx.Connect(ctx, e.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			query := `SELECT rowrelay_outbox.enqueue('events',$1::uuid,$2,$3,
				'[{"key":"binary","value":"AP8="},{"key":"binary","value":null}]')`
			id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012x", n) }
			wire := []byte{0, 255, 0, 128, 1, 2}
			if _, err = tx.Exec(ctx, query, id(1), []byte("held-key"), wire); err != nil {
				t.Fatal(err)
			}
			blocked, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			_, err = e.DB.Exec(blocked, query, id(2), []byte("held-key"), wire)
			stop()
			if err == nil {
				t.Fatal("same-key enqueue overtook an uncommitted predecessor")
			}
			// Cancellation may close pgx's connection; reconnect the independent writer.
			e.DB.Close(ctx)
			e.DB, err = pgx.Connect(ctx, e.URL)
			if err != nil {
				t.Fatal(err)
			}
			for i := 3; i < 20; i++ {
				if _, err = e.DB.Exec(ctx, query, id(i), []byte("independent-key"), wire); err != nil {
					t.Fatal(err)
				}
			}
			read := func(want int) []*kgo.Record {
				t.Helper()
				var records []*kgo.Record
				for len(records) < want && ctx.Err() == nil {
					if mode == "pgque" {
						if _, err = e.DB.Exec(ctx, "SELECT pgque.ticker('rowrelay_outbox.' || epoch::text) FROM rowrelay_outbox.stream"); err != nil {
							t.Fatal(err)
						}
					}
					if _, _, err = r.Step(ctx); err != nil {
						t.Fatal(err)
					}
					poll, stop := context.WithTimeout(ctx, 50*time.Millisecond)
					f := c.PollRecords(poll, want-len(records))
					stop()
					records = append(records, f.Records()...)
				}
				if len(records) != want {
					t.Fatalf("received %d, want %d", len(records), want)
				}
				return records
			}
			for i, record := range read(17) {
				if !bytes.Equal(record.Value, wire) || string(record.Key) != "independent-key" || len(record.Headers) != 3 ||
					!bytes.Equal(record.Headers[0].Value, []byte{0, 255}) || record.Headers[1].Value != nil || string(record.Headers[2].Value) != id(i+3) {
					t.Fatal("wire bytes, headers or per-key enqueue order changed")
				}
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if got := read(1)[0]; string(got.Headers[2].Value) != id(1) {
				t.Fatal("late low-ID commit lost")
			}
			if _, err = e.DB.Exec(ctx, `BEGIN; SELECT rowrelay_outbox.enqueue('events','00000000-0000-0000-0000-000000000099','key'::bytea,'rollback'::bytea); ROLLBACK`); err != nil {
				t.Fatal(err)
			}
			if mode == "pgque" {
				_, err = e.DB.Exec(ctx, "SELECT pgque.ticker('rowrelay_outbox.' || epoch::text) FROM rowrelay_outbox.stream")
				if err != nil {
					t.Fatal(err)
				}
			}
			if n, _, err := r.Step(ctx); err != nil || n != 0 {
				t.Fatalf("rollback escaped: %d %v", n, err)
			}
			if _, err = e.DB.Exec(ctx, query, id(100), []byte("retry-key"), wire); err != nil {
				t.Fatal(err)
			}
			table, condition := "pgque.subscription", "NEW.sub_batch IS NULL AND OLD.sub_batch IS NOT NULL"
			_, err = e.Admin.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION fail_wire_ack() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'injected ACK failure'; END $$;
				CREATE TRIGGER fail_wire_ack BEFORE UPDATE ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION fail_wire_ack()`, table, condition))
			if err != nil {
				t.Fatal(err)
			}
			for ctx.Err() == nil {
				if mode == "pgque" {
					if _, err = e.DB.Exec(ctx, "SELECT pgque.ticker('rowrelay_outbox.' || epoch::text) FROM rowrelay_outbox.stream"); err != nil {
						t.Fatal(err)
					}
				}
				if _, _, err = r.Step(ctx); err != nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if ctx.Err() != nil {
				t.Fatal("ACK failure was not exercised")
			}
			r.Close()
			if _, err = e.Admin.Exec(ctx, "DROP TRIGGER fail_wire_ack ON "+table); err != nil {
				t.Fatal(err)
			}
			r, err = relay.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			for _, record := range read(2) {
				if string(record.Headers[2].Value) != id(100) || !bytes.Equal(record.Value, wire) {
					t.Fatal("replay identity/bytes changed")
				}
			}
		})
	}
}

func TestMixedStreamsKeepProgressWhenOneIsPoisoned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "pgque")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, name := range []string{"healthy", "poison"} {
		if err = outbox.InstallStream(ctx, e.DB, name, e.Name); err != nil {
			t.Fatal(err)
		}
	}
	_, err = e.DB.Exec(ctx, `SELECT pgque.insert_event('rowrelay_outbox.' || epoch::text,'wire','broken') FROM rowrelay_outbox.stream WHERE name='poison';
		SELECT rowrelay_outbox.enqueue('healthy','00000000-0000-0000-0000-000000000001','key'::bytea,'wire'::bytea);
		INSERT INTO items VALUES(1,1,'cdc',NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	// The tick must be a later transaction: its snapshot cannot certify the
	// commit of the transaction in which the test enqueued these records.
	if _, err = e.DB.Exec(ctx, `SELECT pgque.force_next_tick(queue_name) FROM pgque.queue;
		SELECT pgque.ticker(queue_name) FROM pgque.queue`); err != nil {
		t.Fatal(err)
	}
	var healthyTick int64
	if err = e.DB.QueryRow(ctx, `SELECT max(t.tick_id) FROM pgque.tick t JOIN pgque.queue q ON q.queue_id=t.tick_queue
		JOIN rowrelay_outbox.stream o ON q.queue_name='rowrelay_outbox.' || o.epoch::text WHERE o.name='healthy'`).Scan(&healthyTick); err != nil {
		t.Fatal(err)
	}
	c, err := e.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var streams []relay.Stream
	for _, name := range []string{"", "healthy", "poison"} {
		cfg := e.Config()
		cfg.OutboxStream = name
		streams = append(streams, relay.Stream{Name: "stream-" + name, Config: cfg})
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	type observation struct {
		name   string
		failed bool
	}
	observed := make(chan observation, len(streams))
	seen := map[string]bool{} // Report callbacks are serialized by RunStreams.
	go func() {
		done <- relay.RunStreams(runCtx, streams, func(name string, err error) {
			if !seen[name] {
				seen[name] = true
				observed <- observation{name: name, failed: err != nil}
			}
		})
	}()
	defer func() { stop(); <-done }()
	var got []*kgo.Record
	for len(got) < 2 && ctx.Err() == nil {
		got = append(got, c.PollRecords(ctx, 2-len(got)).Records()...)
	}
	if len(got) != 2 {
		t.Fatal("poison stalled independent streams")
	}
	var pending int
	if err = e.DB.QueryRow(ctx, `SELECT count(*) FROM pgque.subscription s JOIN pgque.queue q ON q.queue_id=s.sub_queue
		JOIN rowrelay_outbox.stream o ON q.queue_name='rowrelay_outbox.' || o.epoch::text
		WHERE o.name='poison' AND s.sub_batch IS NOT NULL`).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("poison acknowledged or lost", err)
	}
	for range streams {
		select {
		case status := <-observed:
			if status.failed != (status.name == "stream-poison") {
				t.Fatal("reported open connection as healthy before its first source step", status)
			}
		case <-ctx.Done():
			t.Fatal("missing worker status")
		}
	}
	var healthyAdvanced bool
	if err = e.DB.QueryRow(ctx, `SELECT s.sub_last_tick >= $1 FROM pgque.subscription s JOIN pgque.queue q ON q.queue_id=s.sub_queue
		JOIN rowrelay_outbox.stream o ON q.queue_name='rowrelay_outbox.' || o.epoch::text WHERE o.name='healthy'`, healthyTick).Scan(&healthyAdvanced); err != nil || !healthyAdvanced {
		t.Fatal("worker success reported before source ACK", err)
	}
}
