//go:build integration

package relay_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestManagedInvalidationRoutesWholeSnapshotsAndLateCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "pgque")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	otherTopic := e.Name + "-other"
	admin := kadm.NewClient(e.Kafka)
	if _, err = admin.CreateTopic(ctx, 1, 1, nil, otherTopic); err != nil {
		t.Fatal(err)
	}
	defer admin.DeleteTopic(context.Background(), otherTopic)
	if _, err = e.DB.Exec(ctx, `CREATE SCHEMA other; CREATE TABLE other.items (id bigint PRIMARY KEY);
		SELECT rowrelay.install_capture('other',ARRAY['items']);
		SELECT rowrelay.install_capture('other',ARRAY['items'])`); err != nil {
		t.Fatal(err)
	}
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
	if _, err = e.DB.Exec(ctx, `INSERT INTO items SELECT i,1,'early',NULL FROM generate_series(2,5) i;
		INSERT INTO other.items VALUES(10)`); err != nil {
		t.Fatal(err)
	}
	cfg := e.Config()
	cfg.Topic, cfg.SchemaTopics = "", map[string]string{"public": e.Name, "other": otherTopic}
	cfg.DeliveryMode, cfg.ManagedInvalidation, cfg.ProgressInterval, cfg.Batch = "managed", true, time.Nanosecond, 2
	r, err := relay.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(lab.Broker), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		e.Name: {0: kgo.NewOffset().AtStart()}, otherTopic: {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	step := func(want int) {
		t.Helper()
		if _, err := e.DB.Exec(ctx, "SELECT pgque.force_next_tick('rowrelay'); SELECT pgque.ticker('rowrelay')"); err != nil {
			t.Fatal(err)
		}
		if n, _, err := r.Step(ctx); err != nil || n != want {
			t.Fatal("closed snapshot", n, want, err)
		}
	}
	read := func(count int, want map[string]int) {
		t.Helper()
		seen, certified := map[string]int{}, map[string]bool{}
		for count > 0 {
			fetched := consumer.PollRecords(ctx, count)
			if len(fetched.Errors()) != 0 {
				t.Fatal(fetched.Errors())
			}
			for _, record := range fetched.Records() {
				count--
				var envelope map[string]json.RawMessage
				if err := json.Unmarshal(record.Value, &envelope); err != nil {
					t.Fatal(err)
				}
				if raw, ok := envelope["rowrelay_invalidation_progress"]; ok {
					var progress relay.Progress
					if err := json.Unmarshal(raw, &progress); err != nil || progress.Version != 1 || progress.CommittedBefore.IsZero() {
						t.Fatal("invalid boundary", err)
					}
					if certified[record.Topic] || seen[record.Topic] != want[record.Topic] {
						t.Fatal("boundary passed an incomplete routed prefix")
					}
					certified[record.Topic] = true
				} else {
					if certified[record.Topic] {
						t.Fatal("data followed its own boundary")
					}
					var payload string
					var change relay.Change
					if err := json.Unmarshal(envelope["payload"], &payload); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal([]byte(payload), &change); err != nil || cfg.SchemaTopics[change.Schema] != record.Topic {
						t.Fatal("wrong schema route", err)
					}
					seen[record.Topic]++
				}
			}
		}
		if len(certified) != 2 {
			t.Fatal("both routes need closed boundaries, including an idle route")
		}
	}
	step(5)
	read(7, map[string]int{e.Name: 4, otherTopic: 1})
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	step(1)
	read(3, map[string]int{e.Name: 1, otherTopic: 0})
	if _, err = e.DB.Exec(ctx, `SELECT pgque.insert_event('rowrelay','row',
		'{"schema":"unrouted","table":"items","op":"INSERT","before":null,"after":{"id":1}}');
		SELECT pgque.force_next_tick('rowrelay')`); err != nil {
		t.Fatal(err)
	}
	if _, err = e.DB.Exec(ctx, "SELECT pgque.ticker('rowrelay')"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.Step(ctx); err == nil {
		t.Fatal("unrouted capture was skipped")
	}
	var pending bool
	if err = e.DB.QueryRow(ctx, "SELECT sub_batch IS NOT NULL FROM pgque.subscription").Scan(&pending); err != nil || !pending {
		t.Fatal("unrouted snapshot acknowledged", err)
	}
}

func TestManagedInvalidationLatePublisherCannotUndoAppliedProgress(t *testing.T) {
	for _, role := range []string{"managed-paused", "managed-progress-paused"} {
		t.Run(role, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			e, err := lab.New(ctx, "pgque")
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if _, err = e.DB.Exec(ctx, "INSERT INTO items VALUES(1,1,'first',NULL); UPDATE items SET payload='second' WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			if _, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick('rowrelay'); SELECT pgque.ticker('rowrelay')"); err != nil {
				t.Fatal(err)
			}
			cfg := e.Config()
			cfg.DeliveryMode, cfg.ManagedInvalidation, cfg.ProgressInterval = "managed", true, time.Nanosecond
			config, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := func(role string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, binary, "-test.run=^TestOwnerProcess$", "-test.timeout=25s")
				cmd.Env = append(os.Environ(), "ROWRELAY_TEST_OWNER_ROLE="+role, "ROWRELAY_TEST_OWNER_CONFIG="+string(config))
				return cmd
			}
			old := command(role)
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
			var pid uint32
			if _, scanErr := fmt.Sscanf(line, "READY %d\n", &pid); err != nil || scanErr != nil {
				detail, _ := io.ReadAll(reader)
				t.Fatal("old owner did not pause", line, string(detail), err)
			}
			if _, err = e.Admin.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
				t.Fatal(err)
			}
			count := 4
			if role == "managed-progress-paused" {
				count = 6
				if _, err = e.DB.Exec(ctx, "UPDATE items SET payload='third' WHERE id=1; UPDATE items SET payload='fourth' WHERE id=1"); err != nil {
					t.Fatal(err)
				}
				if _, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick('rowrelay'); SELECT pgque.ticker('rowrelay')"); err != nil {
					t.Fatal(err)
				}
			}
			if output, err := command("new").CombinedOutput(); err != nil || !strings.Contains(string(output), "DELIVERED") {
				t.Fatal("successor", err, string(output))
			}
			if _, err = resume.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			if line, err = reader.ReadString('\n'); err != nil || strings.TrimSpace(line) != "REPLAYED" {
				t.Fatal("late publish not exercised", line, err)
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
			for len(records) < count {
				fetched := consumer.PollRecords(ctx, count-len(records))
				if len(fetched.Errors()) != 0 {
					t.Fatal(fetched.Errors())
				}
				records = append(records, fetched.Records()...)
			}
			if role == "managed-paused" {
				if string(records[0].Value) != string(records[3].Value) || !strings.Contains(string(records[2].Value), "rowrelay_invalidation_progress") {
					t.Fatal("expected data,data,boundary,late duplicate")
				}
			} else {
				var newer, older struct {
					Progress relay.Progress `json:"rowrelay_invalidation_progress"`
				}
				if json.Unmarshal(records[4].Value, &newer) != nil || json.Unmarshal(records[5].Value, &older) != nil || !older.Progress.CommittedBefore.Before(newer.Progress.CommittedBefore) {
					t.Fatal("older boundary did not arrive after successor progress")
				}
			}
		})
	}
}
