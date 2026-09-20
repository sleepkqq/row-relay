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

func TestClosedProgressIncludesLateCommitAndStopsOnApplyFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "pgque")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var epoch string
	if err = e.DB.QueryRow(ctx, "SELECT epoch::text FROM rowrelay.source").Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	tracker, err := relay.NewProgressTracker(e.Name, epoch, [][2]string{{"public", "items"}}, 5*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan int, 16)
	done := make(chan error, 1)
	finished := make(chan struct{})
	readCtx, stop := context.WithCancel(ctx)
	defer func() { stop(); <-finished }()
	go func() {
		defer close(finished)
		done <- tracker.Follow(readCtx, []string{lab.Broker}, relay.KafkaSecurity{}, func(_ context.Context, change relay.Change) error {
			var row struct{ ID int }
			if err := json.Unmarshal(change.After, &row); err != nil {
				return err
			}
			if row.ID == 99 {
				return errors.New("injected application failure")
			}
			seen <- row.ID
			return nil
		})
	}()
	config := e.Config()
	config.ProgressInterval, config.Batch = time.Nanosecond, 2
	r, err := relay.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	other, err := pgx.Connect(ctx, e.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(context.Background())
	tx, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "INSERT INTO items VALUES(1,1,'late',NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.DB.Exec(ctx, "INSERT INTO items SELECT i,1,'early',NULL FROM generate_series(2,8) i"); err != nil {
		t.Fatal(err)
	}
	step := func(want int) {
		t.Helper()
		if _, err := e.DB.Exec(ctx, "SELECT pgque.force_next_tick('rowrelay'); SELECT pgque.ticker('rowrelay')"); err != nil {
			t.Fatal(err)
		}
		if n, _, err := r.Step(ctx); err != nil || n != want {
			t.Fatalf("step: got %d, want %d, error %v", n, want, err)
		}
	}
	read := func(want int) {
		t.Helper()
		select {
		case id := <-seen:
			if id != want {
				t.Fatalf("application order: got %d, want %d", id, want)
			}
		case err := <-done:
			t.Fatal("consumer stopped", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	step(7) // More than one Kafka chunk; the open low-ID transaction is excluded.
	for id := 2; id <= 8; id++ {
		read(id)
	}
	for !tracker.Ready() {
		select {
		case err := <-done:
			t.Fatal("consumer stopped before first certificate", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	step(1)
	read(1) // A lower ID committed later remains deliverable after a certificate.
	if _, err = e.DB.Exec(ctx, "INSERT INTO items VALUES(99,1,'apply-fails',NULL)"); err != nil {
		t.Fatal(err)
	}
	step(1) // Publishes a later certificate too; application failure must block it.
	select {
	case err := <-done:
		if err == nil || tracker.Ready() {
			t.Fatal("failed apply passed a later certificate", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestReferenceConsumerFailsWhenRetentionOvertakesItsCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "control")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tracker, err := relay.NewProgressTracker(e.Name, "epoch", [][2]string{{"public", "items"}}, 5*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(lab.Broker), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	readCtx, stop := context.WithCancel(ctx)
	defer func() { stop(); <-finished }()
	go func() {
		defer close(finished)
		first := true
		done <- tracker.Follow(readCtx, []string{lab.Broker}, relay.KafkaSecurity{}, func(ctx context.Context, _ relay.Change) error {
			if first {
				first = false
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}()
	value, _ := json.Marshal(map[string]any{"rowrelay_progress": relay.Progress{Version: 1, SourceEpoch: "epoch", CommittedBefore: time.Now().UTC()}})
	if err = producer.ProduceSync(ctx, &kgo.Record{Topic: e.Name, Key: []byte("epoch"), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for !tracker.Ready() {
		select {
		case err := <-done:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	// Uncompressed data greatly exceeds the reader's one-fetch/1 MiB budget.
	// Blocking its first callback prevents it from buffering the entire log.
	records := make([]*kgo.Record, 1000)
	for i := range records {
		raw := []byte(fmt.Sprintf(`{"schema":"public","table":"items","op":"INSERT","after":{"id":%d,"body":"%s"}}`, i+1, strings.Repeat("x", 16384)))
		value, err := relay.Encode("epoch", int64(i+1), raw)
		if err != nil {
			t.Fatal(err)
		}
		records[i] = &kgo.Record{Topic: e.Name, Key: []byte("epoch"), Value: value}
	}
	if err = producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case err := <-done:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	admin := kadm.NewClient(e.Kafka)
	ends, err := admin.ListEndOffsets(ctx, e.Name)
	if err != nil || ends[e.Name][0].Err != nil {
		t.Fatal("cannot get deletion boundary", err)
	}
	end := ends[e.Name][0].Offset
	deleted, err := admin.DeleteRecords(ctx, kadm.Offsets{e.Name: {0: {Topic: e.Name, Partition: 0, At: end}}})
	result, exists := deleted.Lookup(e.Name, 0)
	if err != nil || !exists || result.Err != nil || result.LowWatermark != end {
		t.Fatal("retention fixture did not advance log start", err, result)
	}
	close(release)
	select {
	case err := <-done:
		if err == nil || tracker.Ready() {
			t.Fatal("reader silently skipped a retention gap", err)
		}
	case <-ctx.Done():
		t.Fatal("reader did not fail on a real offset gap", ctx.Err())
	}
}

func TestClosedProgressCannotPassPoisonOrExternalTicker(t *testing.T) {
	for _, failure := range []string{"poison", "external-ticker"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			e, err := lab.New(ctx, "pgque")
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if _, err := e.DB.Exec(ctx, "INSERT INTO items SELECT i,1,'valid-prefix',NULL FROM generate_series(1,2) i"); err != nil {
				t.Fatal(err)
			}
			if failure == "poison" {
				if _, err := e.DB.Exec(ctx, "SELECT pgque.insert_event('rowrelay','rowrelay','invalid-json')"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := e.DB.Exec(ctx, "SELECT pgque.force_next_tick('rowrelay'); SELECT pgque.ticker('rowrelay')"); err != nil {
				t.Fatal(err)
			}
			if failure == "external-ticker" {
				if _, err := e.DB.Exec(ctx, "UPDATE pgque.queue SET queue_external_ticker=true WHERE queue_name='rowrelay'"); err != nil {
					t.Fatal(err)
				}
			}
			config := e.Config()
			config.ProgressInterval, config.Batch = time.Nanosecond, 1
			r, err := relay.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, _, err := r.Step(ctx); err == nil {
				t.Fatal("invalid batch was certified")
			}
			if _, _, err := r.Step(ctx); err == nil {
				t.Fatal("failed publisher resumed without reacquiring ownership")
			}
			var pending bool
			if err := e.DB.QueryRow(ctx, "SELECT sub_batch IS NOT NULL FROM pgque.subscription").Scan(&pending); err != nil || !pending {
				t.Fatal("failed batch was acknowledged", err)
			}
			ends, err := kadm.NewClient(e.Kafka).ListEndOffsets(ctx, e.Name)
			if err != nil || ends[e.Name][0].Err != nil {
				t.Fatal("cannot obtain final Kafka boundary", err)
			}
			reader, err := e.Consumer(kgo.KeepControlRecords())
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			var next int64
			data := 0
			for next < ends[e.Name][0].Offset {
				fetches := reader.PollRecords(ctx, 10)
				if errs := fetches.Errors(); len(errs) != 0 {
					t.Fatal(errs)
				}
				for _, record := range fetches.Records() {
					next = record.Offset + 1
					if record.Attrs.IsControl() {
						continue
					}
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(record.Value, &envelope); err != nil || envelope["rowrelay_progress"] != nil {
						t.Fatal("progress overtook the failure", err)
					}
					data++
				}
			}
			if failure == "poison" && data != 1 || failure == "external-ticker" && data != 0 {
				t.Fatalf("unexpected visible data prefix: %d", data)
			}
		})
	}
}
