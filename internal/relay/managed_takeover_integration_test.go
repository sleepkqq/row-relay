//go:build integration

package relay_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestManagedTakeoverDeduplicatesLatePublisherEffects(t *testing.T) {
	for _, failure := range []string{"terminate", "idle-expiry"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			e, err := lab.New(ctx, "outbox-pgque")
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			_, err = e.DB.Exec(ctx, `SELECT rowrelay_outbox.enqueue('events',
				'00000000-0000-0000-0000-000000000001','same-key'::bytea,'first'::bytea);
				SELECT rowrelay_outbox.enqueue('events',
				'00000000-0000-0000-0000-000000000002','same-key'::bytea,'second'::bytea);
				CREATE TABLE inbox (id uuid PRIMARY KEY, digest text NOT NULL);
				CREATE TABLE effects (seq bigint GENERATED ALWAYS AS IDENTITY, value text NOT NULL)`)
			if err != nil {
				t.Fatal(err)
			}
			// The closed snapshot must be created AFTER the enqueue transaction commits.
			if _, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick(queue_name) FROM pgque.queue; SELECT pgque.ticker(queue_name) FROM pgque.queue"); err != nil {
				t.Fatal(err)
			}
			cfg := e.Config()
			cfg.OutboxStream, cfg.DeliveryMode, cfg.ManagedTakeover = "events", "managed", true
			cfg.Timeout = 2 * time.Second
			config, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := func(role string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, binary, "-test.run=^TestOwnerProcess$", "-test.timeout=30s")
				cmd.Env = append(os.Environ(), "ROWRELAY_TEST_OWNER_ROLE="+role, "ROWRELAY_TEST_OWNER_CONFIG="+string(config))
				return cmd
			}
			old := command("managed-paused")
			output, err := old.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			resume, err := old.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = old.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { resume.Close(); old.Process.Kill(); old.Wait() }()
			reader := bufio.NewReader(output)
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal("old owner did not pause", err, line)
			}
			var pid uint32
			if _, err = fmt.Sscanf(line, "READY %d\n", &pid); err != nil {
				detail, _ := io.ReadAll(reader)
				t.Fatal("old owner startup", line, string(detail))
			}
			if failure == "terminate" {
				var terminated bool
				if err = e.Admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
					t.Fatal("terminate owner session", err)
				}
			} else {
				for {
					var alive bool
					if err = e.Admin.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", pid).Scan(&alive); err != nil {
						t.Fatal(err)
					}
					if !alive {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("managed owner did not expire")
					case <-time.After(50 * time.Millisecond):
					}
				}
			}
			if output, err := command("new").CombinedOutput(); err != nil || !strings.Contains(string(output), "DELIVERED") {
				t.Fatal("successor", err, string(output))
			}
			if _, err = resume.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			line, err = reader.ReadString('\n')
			if err != nil || strings.TrimSpace(line) != "REPLAYED" {
				t.Fatal("late publish/source ACK rejection not exercised", err, line)
			}
			if err = old.Wait(); err != nil {
				t.Fatal(err)
			}
			consumer, err := e.Consumer()
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			var records []*kgo.Record
			for len(records) < 3 {
				fetched := consumer.PollRecords(ctx, 3-len(records))
				if len(fetched.Errors()) != 0 {
					t.Fatal(fetched.Errors())
				}
				records = append(records, fetched.Records()...)
			}
			if string(records[0].Value) != "first" || string(records[1].Value) != "second" || string(records[2].Value) != "first" {
				t.Fatal("expected a visible stale duplicate AFTER the successor's newer event")
			}
			if err = applyInbox(ctx, e.DB, records[0], true); err == nil {
				t.Fatal("consumer rollback not exercised")
			}
			for _, record := range records {
				if err = applyInbox(ctx, e.DB, record, false); err != nil {
					t.Fatal(err)
				}
			}
			conflicting := *records[0]
			conflicting.Value = []byte("changed-under-same-ID")
			if err = applyInbox(ctx, e.DB, &conflicting, false); err == nil {
				t.Fatal("conflicting replay accepted")
			}
			var effects []string
			if err = e.DB.QueryRow(ctx, "SELECT array_agg(value ORDER BY seq) FROM effects").Scan(&effects); err != nil || strings.Join(effects, ",") != "first,second" {
				t.Fatal("duplicate or reordered business effects", effects, err)
			}
		})
	}
}

// Reference consumer contract: receipt, content check and domain mutation use
// ONE database transaction. An external Redis SETNX cannot provide this atomicity.
func applyInbox(ctx context.Context, db *pgx.Conn, record *kgo.Record, crash bool) error {
	if len(record.Headers) != 1 || record.Headers[0].Key != "id" || string(record.Key) != "same-key" {
		return errors.New("invalid fixture envelope")
	}
	content, err := json.Marshal([][]byte{record.Key, record.Value})
	if err != nil {
		return err
	}
	digest, id := fmt.Sprintf("%x", sha256.Sum256(content)), string(record.Headers[0].Value)
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	tag, err := tx.Exec(ctx, "INSERT INTO inbox VALUES($1,$2) ON CONFLICT DO NOTHING", id, digest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var previous string
		if err = tx.QueryRow(ctx, "SELECT digest FROM inbox WHERE id=$1", id).Scan(&previous); err != nil {
			return err
		}
		if previous != digest {
			return errors.New("conflicting delivery ID")
		}
	} else if _, err = tx.Exec(ctx, "INSERT INTO effects(value) VALUES($1)", string(record.Value)); err != nil {
		return err
	}
	if crash {
		return errors.New("simulated failure after domain mutation, before commit")
	}
	return tx.Commit(ctx)
}
