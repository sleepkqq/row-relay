//go:build integration

package relay_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/capture"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
)

func TestCaptureRejectsUnsupportedRelationsAtomically(t *testing.T) {
	for name, ddl := range map[string]string{
		"unlogged":    "CREATE UNLOGGED TABLE unsupported(id bigint PRIMARY KEY)",
		"temporary":   "CREATE TEMP TABLE unsupported(id bigint PRIMARY KEY)",
		"no-key":      "CREATE TABLE unsupported(id bigint)",
		"partitioned": "CREATE TABLE unsupported(id bigint PRIMARY KEY) PARTITION BY RANGE(id)",
		"inheritance": "CREATE TABLE unsupported(id bigint PRIMARY KEY); CREATE TABLE inherited() INHERITS(unsupported)",
		"view":        "CREATE VIEW unsupported AS SELECT id FROM items",
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			// This installs PgQue without installing a CDC source.
			e, err := lab.New(ctx, "outbox-pgque")
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if _, err := e.DB.Exec(ctx, ddl); err != nil {
				t.Fatal(err)
			}
			schema := "public"
			if name == "temporary" {
				schema = "pg_temp"
			}
			if err := capture.Install(ctx, e.DB, [][2]string{{"public", "items"}, {schema, "unsupported"}}); err == nil {
				t.Fatal("unsupported relation accepted")
			}
			var clean bool
			if err := e.DB.QueryRow(ctx, `SELECT to_regnamespace('rowrelay') IS NULL
				AND NOT EXISTS (SELECT FROM pgque.queue WHERE queue_name='rowrelay')
				AND NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid='items'::regclass AND tgname='rowrelay_capture')`).Scan(&clean); err != nil || !clean {
				t.Fatal("failed installation left partial schema, queue or triggers", err)
			}
		})
	}
}

func TestCapturePreservesCascadesAndRejectsTruncate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "outbox-pgque")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.DB.Exec(ctx, `CREATE TABLE parent_row(id bigint PRIMARY KEY);
		CREATE TABLE child_row(id bigint PRIMARY KEY, fk bigint REFERENCES parent_row(id)
		ON UPDATE CASCADE ON DELETE CASCADE, amount numeric, "binary" bytea, label text, nullable text)`); err != nil {
		t.Fatal(err)
	}
	if err := capture.Install(ctx, e.DB, [][2]string{{"public", "parent_row"}, {"public", "child_row"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.DB.Exec(ctx, `INSERT INTO parent_row VALUES(9007199254740993);
		INSERT INTO child_row VALUES(1,9007199254740993,12345678901234567890.12345,
		decode('0001ff','hex'),'café/Москва',NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"TRUNCATE child_row", "TRUNCATE parent_row CASCADE"} {
		if _, err := e.DB.Exec(ctx, sql); err == nil {
			t.Fatal("TRUNCATE bypassed capture protection")
		}
	}
	if _, err := e.DB.Exec(ctx, `UPDATE parent_row SET id=9007199254740994;
		DELETE FROM parent_row WHERE id=9007199254740994;
		SELECT pgque.force_next_tick('rowrelay')`); err != nil {
		t.Fatal(err)
	}
	// Take the snapshot after the writing transaction commits.
	if _, err := e.DB.Exec(ctx, "SELECT pgque.ticker('rowrelay')"); err != nil {
		t.Fatal(err)
	}
	config := e.Config()
	config.Batch = 2
	r, err := relay.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if n, _, err := r.Step(ctx); err != nil || n != 6 {
		t.Fatalf("cascade mutations: count=%d, error=%v", n, err)
	}
	reader, err := e.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := map[string]bool{}
	for len(seen) < 6 {
		fetches := reader.PollRecords(ctx, 6)
		if errs := fetches.Errors(); len(errs) != 0 {
			t.Fatal(errs)
		}
		for _, record := range fetches.Records() {
			var envelope struct{ Payload string }
			var change relay.Change
			if json.Unmarshal(record.Value, &envelope) != nil || json.Unmarshal([]byte(envelope.Payload), &change) != nil {
				t.Fatal("invalid CDC envelope")
			}
			key := change.Table + ":" + change.Op
			if seen[key] {
				t.Fatal("duplicate mutation", key)
			}
			seen[key] = true
			type image struct {
				ID, FK, Amount json.Number
				Binary, Label  string
				Nullable       *string
			}
			var before, after *image
			if json.Unmarshal(change.Before, &before) != nil || json.Unmarshal(change.After, &after) != nil {
				t.Fatal("invalid row image")
			}
			if change.Table == "child_row" {
				for _, row := range []*image{before, after} {
					if row != nil && (row.Amount.String() != "12345678901234567890.12345" || row.Binary != `\x0001ff` || row.Label != "café/Москва" || row.Nullable != nil) {
						t.Fatal("typed value or NULL changed")
					}
				}
				if change.Op == "UPDATE" && (before == nil || after == nil || before.FK.String() != "9007199254740993" || after.FK.String() != "9007199254740994") {
					t.Fatal("cascade lost old/new association")
				}
			}
			if change.Table == "parent_row" && change.Op == "UPDATE" &&
				(before == nil || after == nil || before.ID.String() != "9007199254740993" || after.ID.String() != "9007199254740994") {
				t.Fatal("primary-key move lost precision or images")
			}
		}
	}
}
