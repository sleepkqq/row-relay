// Package outbox accepts complete wire records without interpreting their schema.
package outbox

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

//go:embed schema.sql
var schema string

var streamName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
var deliveryID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type Message struct {
	ID      string   `json:"id"`
	Key     []byte   `json:"key"`
	Value   []byte   `json:"value"`
	Headers []Header `json:"headers"`
}

// Decode preserves wire bytes, including duplicate non-ID headers and null values.
// Invalid source data blocks delivery; driver/decoder details never become logs.
func Decode(raw []byte) (*kgo.Record, error) {
	var m Message
	invalid := errors.New("invalid business-outbox record")
	if len(raw) > 786432 || json.Unmarshal(raw, &m) != nil || !deliveryID.MatchString(m.ID) ||
		len(m.Key) == 0 || len(m.Key) > 4096 || len(m.Value) == 0 || len(m.Value) > 524288 {
		return nil, invalid
	}
	r := &kgo.Record{Key: m.Key, Value: m.Value}
	idHeaders, headerBytes := 0, 0
	for _, h := range m.Headers {
		if len(h.Key) == 0 || len(h.Key) > 256 {
			return nil, invalid
		}
		headerBytes += len(h.Key) + len(h.Value)
		if h.Key == "id" {
			idHeaders++
			if !strings.EqualFold(string(h.Value), m.ID) {
				return nil, invalid
			}
		}
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	if idHeaders != 1 || headerBytes > 16384 {
		return nil, invalid
	}
	return r, nil
}

// InstallStream creates the explicit v1 schema once and registers a new immutable
// routing entry. Re-registration fails rather than resetting a live source.
func InstallStream(ctx context.Context, db *pgx.Conn, name, topic string) error {
	if !streamName.MatchString(name) || len(topic) == 0 || len(topic) > 249 {
		return errors.New("invalid outbox stream or topic")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(1380931405)"); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT to_regnamespace('rowrelay_outbox') IS NOT NULL").Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err = tx.Exec(ctx, schema); err != nil {
			return err
		}
	}
	var version int
	if err = tx.QueryRow(ctx, "SELECT version FROM rowrelay_outbox.version").Scan(&version); err != nil || version != 1 {
		return errors.New("unsupported outbox installation")
	}
	var epoch string
	if err = tx.QueryRow(ctx, `INSERT INTO rowrelay_outbox.stream(name,queue_mode,topic)
		VALUES ($1,'pgque',$2) RETURNING epoch::text`, name, topic).Scan(&epoch); err != nil {
		return err
	}
	queue := "rowrelay_outbox." + epoch
	for _, query := range []string{
		"SELECT pgque.create_queue($1)",
		"SELECT pgque.register_consumer($1, 'rowrelay')",
		"SELECT pgque.set_queue_config($1, 'ticker_max_count', '1')",
		"SELECT pgque.set_queue_config($1, 'ticker_max_lag', '10 milliseconds')",
		"SELECT pgque.set_queue_config($1, 'ticker_idle_period', '100 milliseconds')",
	} {
		if _, err = tx.Exec(ctx, query, queue); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
