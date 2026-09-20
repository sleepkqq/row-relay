//go:build integration

package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCLIShutdownDuringSourceAck(t *testing.T) {
	binary, err := filepath.Abs("../../bin/row-relay")
	if err != nil {
		t.Fatal(err)
	}
	for _, signal := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		for _, mode := range []string{"pgque"} {
			for _, wire := range []bool{false, true} {
				fixture := mode
				if wire {
					fixture = "outbox-" + mode
				}
				t.Run(fmt.Sprintf("%s/%s", signal, fixture), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					e, err := lab.New(ctx, fixture)
					if err != nil {
						t.Fatal(err)
					}
					defer e.Close()
					write := lab.Write
					if wire {
						write = lab.WriteOutbox
					}
					if err := write(ctx, e.DB, []int64{1, 2}, []int64{1, 2}, []string{"first", "second"}); err != nil {
						t.Fatal(err)
					}
					// A server-side latch holds source ACK after Kafka has committed.
					// This key is disjoint from the source ownership lock.
					table, predicate := "pgque.subscription", "OLD.sub_batch IS NOT NULL AND NEW.sub_batch IS NULL"
					_, err = e.Admin.Exec(ctx, `CREATE FUNCTION public.block_ack() RETURNS trigger LANGUAGE plpgsql AS $$
						BEGIN PERFORM pg_advisory_xact_lock(1380931405::bigint); RETURN NEW; END $$;
						CREATE TRIGGER block_ack BEFORE UPDATE ON `+table+` FOR EACH ROW WHEN (`+predicate+`) EXECUTE FUNCTION public.block_ack()`)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := e.DB.Exec(ctx, "SELECT pg_advisory_lock(1380931405::bigint)"); err != nil {
						t.Fatal(err)
					}
					args := []string{"--topic", e.Name, "--poll", "10ms", "--cdc-format", "legacy-json"}
					if wire {
						args = append(args, "--outbox-stream", "events")
					}
					cmd := exec.CommandContext(ctx, binary, args...)
					cmd.Env = append(os.Environ(), "DATABASE_URL="+e.URL, "KAFKA_BROKERS="+lab.Broker,
						"KAFKA_SECURITY_PROTOCOL=PLAINTEXT", "KAFKA_SASL_USERNAME=", "KAFKA_SASL_PASSWORD=", "KAFKA_TLS_CA_FILE=")
					var log bytes.Buffer
					cmd.Stdout, cmd.Stderr = &log, &log
					if err := cmd.Start(); err != nil {
						t.Fatal("run make integration to build the CLI first", err)
					}
					done := make(chan struct{})
					var exitErr error
					go func() { exitErr = cmd.Wait(); close(done) }()
					defer func() { _ = cmd.Process.Kill(); <-done }()
					for {
						var blocked bool
						if err := e.Admin.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_locks
							WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid=1380931405
							AND objsubid=1 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`).Scan(&blocked); err != nil {
							t.Fatal(err)
						}
						if blocked {
							break
						}
						select {
						case <-done:
							t.Fatalf("CLI exited before source ACK latch: %v %s", exitErr, log.String())
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(10 * time.Millisecond):
						}
					}
					consumer, err := e.Consumer()
					if err != nil {
						t.Fatal(err)
					}
					defer consumer.Close()
					read := func() []*kgo.Record {
						t.Helper()
						var records []*kgo.Record
						for len(records) < 2 {
							fetch := consumer.PollRecords(ctx, 2-len(records))
							if errs := fetch.Errors(); len(errs) != 0 {
								t.Fatal(errs)
							}
							records = append(records, fetch.Records()...)
						}
						return records
					}
					before := read() // Proves that the lost source ACK follows broker commit.
					if err := cmd.Process.Signal(signal); err != nil {
						t.Fatal(err)
					}
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("CLI did not stop within its shutdown bound")
					}
					if signal == syscall.SIGTERM && exitErr != nil {
						t.Fatalf("SIGTERM was not graceful: %v %s", exitErr, log.String())
					}
					if _, err := e.DB.Exec(ctx, "SELECT pg_advisory_unlock(1380931405::bigint)"); err != nil {
						t.Fatal(err)
					}
					cfg := e.Config()
					if wire {
						cfg.OutboxStream = "events"
					}
					runner, err := relay.Open(ctx, cfg)
					if err != nil {
						t.Fatal("successor could not acquire ownership", err)
					}
					defer runner.Close()
					if n, _, err := runner.Step(ctx); err != nil || n != 2 {
						t.Fatalf("source was lost or acknowledged before shutdown: n=%d err=%v", n, err)
					}
					for i, after := range read() {
						if !bytes.Equal(before[i].Key, after.Key) || !bytes.Equal(before[i].Value, after.Value) ||
							!reflect.DeepEqual(before[i].Headers, after.Headers) {
							t.Fatal("shutdown replay changed identity, bytes or ordering")
						}
					}
					if n, _, err := runner.Step(ctx); err != nil || n != 0 {
						t.Fatalf("successor did not finish source ACK: n=%d err=%v", n, err)
					}
				})
			}
		}
	}
}
