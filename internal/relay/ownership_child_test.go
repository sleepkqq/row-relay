//go:build integration

package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestOwnerProcess is executed in real subprocesses by the takeover test. Its
// old-owner pause sits between broker Produce ACK and transaction commit.
func TestOwnerProcess(t *testing.T) {
	role := os.Getenv("ROWRELAY_TEST_OWNER_ROLE")
	if role == "" {
		t.Skip("subprocess helper")
	}
	var cfg Config
	if err := json.Unmarshal([]byte(os.Getenv("ROWRELAY_TEST_OWNER_CONFIG")), &cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	r, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if role == "managed-progress-paused" {
		if !cfg.ManagedInvalidation {
			t.Fatal("invalidation-only helper requires explicit profile")
		}
		var boundary time.Time
		if err = r.db.QueryRow(ctx, "SELECT batch_end FROM pgque.get_batch_info(pgque.next_batch($1,'rowrelay'))", r.queue).Scan(&boundary); err != nil {
			t.Fatal(err)
		}
		r.config.ProgressInterval = 0 // Pause the real protocol between source ACK/ownership check and control Produce.
		if n, _, err := r.Step(ctx); err != nil || n != 2 {
			t.Fatal("old closed prefix", n, err)
		}
		var owned bool
		if err = r.db.QueryRow(ctx, "SELECT ("+ownershipQuery+")", r.lockID).Scan(&owned); err != nil || !owned {
			t.Fatal("old checkpoint ownership", err)
		}
		value, err := json.Marshal(map[string]Progress{"rowrelay_invalidation_progress": {1, r.epoch, boundary.UTC()}})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("READY %d\n", r.db.PgConn().PID())
		if _, err = io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		if err = r.publish(ctx, []*kgo.Record{{Topic: cfg.Topic, Key: []byte(r.epoch), Value: value}}); err != nil {
			t.Fatal("late old boundary was not exercised", err)
		}
		if err = r.publishProgress(ctx, time.Now()); err == nil {
			t.Fatal("lost owner created another boundary")
		}
		fmt.Println("REPLAYED")
		return
	}
	if role == "lost-kafka-ack" {
		r.kafka.Close()
		dial, fired := kafkaCommitReplyLoss(t, ctx, cfg.Brokers[0])
		r.kafka, err = kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.TransactionalID("rowrelay-"+r.epoch),
			kgo.RequiredAcks(kgo.AllISRAcks()), kgo.Dialer(dial))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = r.kafka.ProducerID(ctx); err != nil {
			t.Fatal(err)
		}
		step, stop := context.WithTimeout(ctx, 2*time.Second)
		_, _, err = r.Step(step)
		stop()
		if err == nil {
			t.Fatal("unconfirmed Kafka commit acknowledged source")
		}
		select {
		case <-fired:
		default:
			t.Fatal("Kafka commit response loss was not exercised")
		}
		if !r.failed {
			t.Fatal("ambiguous producer remained reusable")
		}
		fmt.Println("UNCONFIRMED")
		return
	}
	if role == "new" {
		delivered := 0
		for delivered < 2 && ctx.Err() == nil {
			n, _, err := r.Step(ctx)
			if err != nil {
				t.Fatal(err)
			}
			delivered += n
		}
		if delivered != 2 {
			t.Fatal("successor failed to deliver source")
		}
		fmt.Println("DELIVERED")
		return
	}
	var id int64
	var raw []byte
	var batchID *int64
	{
		if err = r.db.QueryRow(ctx, "SELECT pgque.next_batch($1,'rowrelay')", r.queue).Scan(&batchID); err != nil || batchID == nil {
			t.Fatal("no source batch", err)
		}
		var sql string
		if err = r.db.QueryRow(ctx, "SELECT pgque.batch_event_sql($1)", *batchID).Scan(&sql); err != nil {
			t.Fatal(err)
		}
		err = r.db.QueryRow(ctx, "SELECT ev_id,ev_data FROM ("+sql+") s ORDER BY ev_id LIMIT 1", pgx.QueryExecModeExec).Scan(&id, &raw)
	}
	if err != nil {
		t.Fatal(err)
	}
	var record *kgo.Record
	if cfg.OutboxStream != "" {
		record, err = outbox.Decode(raw)
	} else {
		value, encodeErr := Encode(r.epoch, id, raw)
		err = encodeErr
		record = &kgo.Record{Key: []byte(r.epoch), Value: value}
	}
	if err != nil {
		t.Fatal(err)
	}
	record.Topic = cfg.Topic
	if role == "managed-paused" {
		if cfg.DeliveryMode != "managed" || (!cfg.ManagedTakeover && !cfg.ManagedInvalidation) {
			t.Fatal("managed replay helper requires explicit takeover")
		}
		fmt.Printf("READY %d\n", r.db.PgConn().PID())
		if _, err = io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		// Deliberately exercise the unfenceable window: a live Kafka connection
		// may accept this old record AFTER the successor has ACKed the whole batch.
		if err = r.publish(ctx, []*kgo.Record{record}); err != nil {
			t.Fatal("late managed publish was not exercised", err)
		}
		if err = r.acknowledge(ctx, *batchID); err == nil {
			t.Fatal("stale managed publisher acknowledged source")
		}
		if cfg.ManagedInvalidation && r.publishProgress(ctx, time.Now()) == nil {
			t.Fatal("stale publisher certified an unacknowledged prefix")
		}
		fmt.Println("REPLAYED")
		return
	}
	if err = r.kafka.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err = r.kafka.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if role == "unlock-before-ack" {
		if err = r.kafka.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}
		if _, err = r.db.Exec(ctx, "SELECT pg_advisory_unlock_all()"); err != nil {
			t.Fatal(err)
		}
		if err = r.acknowledge(ctx, *batchID); err == nil {
			t.Fatal("live but unowned connection acknowledged source")
		}
		fmt.Println("UNCONFIRMED")
		return
	}
	fmt.Printf("READY %d\n", r.db.PgConn().PID())
	if _, err = io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err = r.kafka.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Fatal("stale producer committed after takeover")
	}
	if err = r.acknowledge(ctx, *batchID); err == nil {
		t.Fatal("stale producer acknowledged source")
	}
	fmt.Println("FENCED")
}
