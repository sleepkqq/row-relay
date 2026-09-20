// Package lab is shared by the disposable integration and benchmark harnesses.
package lab

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/capture"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/sleepkqq/row-relay/internal/pgque"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Broker = "127.0.0.1:29092"
const adminURL = "postgres://lab:lab@127.0.0.1:25432/rowrelay?sslmode=disable"

type Env struct {
	Name   string
	URL    string
	DB     *pgx.Conn
	Admin  *pgx.Conn
	Kafka  *kgo.Client
	parent *pgx.Conn
}

func New(ctx context.Context, mode string) (*Env, error) {
	return NewWithReplication(ctx, mode, 1)
}

func NewWithReplication(ctx context.Context, mode string, replicationFactor int16) (e *Env, err error) {
	if replicationFactor < 1 || replicationFactor > 3 {
		return nil, fmt.Errorf("invalid lab replication factor: %d", replicationFactor)
	}
	wire := strings.HasPrefix(mode, "outbox-")
	mode = strings.TrimPrefix(mode, "outbox-")
	if mode != "pgque" && mode != "control" {
		return nil, fmt.Errorf("unsupported lab fixture: %s", mode)
	}
	e = &Env{Name: fmt.Sprintf("rrlab_%d", time.Now().UnixNano())}
	created := e
	defer func() {
		if err != nil {
			created.Close()
		}
	}()
	e.parent, err = pgx.Connect(ctx, adminURL)
	if err != nil {
		return nil, err
	}
	_, err = e.parent.Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='rowrelay_owner') THEN
			CREATE ROLE rowrelay_owner LOGIN PASSWORD 'lab' NOSUPERUSER NOCREATEROLE NOREPLICATION;
		END IF;
	END $$`)
	if err != nil {
		return nil, err
	}
	_, err = e.parent.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{e.Name}.Sanitize()+" OWNER rowrelay_owner")
	if err != nil {
		return nil, err
	}
	e.Admin, err = pgx.Connect(ctx, "postgres://lab:lab@127.0.0.1:25432/"+e.Name+"?sslmode=disable")
	if err != nil {
		return nil, err
	}
	if _, err = e.Admin.Exec(ctx, "CREATE EXTENSION pg_stat_statements"); err != nil {
		return nil, err
	}
	if mode == "pgque" {
		if err = pgque.Install(ctx, e.Admin); err != nil {
			return nil, err
		}
		if _, err = e.Admin.Exec(ctx, "GRANT pgque_admin TO rowrelay_owner"); err != nil {
			return nil, err
		}
	}
	e.URL = "postgres://rowrelay_owner:lab@127.0.0.1:25432/" + e.Name + "?sslmode=disable"
	e.DB, err = pgx.Connect(ctx, e.URL)
	if err != nil {
		return nil, err
	}
	_, err = e.DB.Exec(ctx, `CREATE TABLE items (
		id bigint PRIMARY KEY, sent_ns bigint NOT NULL, payload text NOT NULL, optional text);
		CREATE TABLE expected (id bigint PRIMARY KEY, sent_ns bigint NOT NULL, checksum text NOT NULL)`)
	if err != nil {
		return nil, err
	}
	if mode != "control" && !wire {
		if err = capture.Install(ctx, e.DB, [][2]string{{"public", "items"}}); err != nil {
			return nil, err
		}
	}
	e.Kafka, err = kgo.NewClient(kgo.SeedBrokers(Broker))
	if err != nil {
		return nil, err
	}
	// Compose --wait only waits for a running container when its image has no
	// healthcheck. Require a successful Kafka metadata request before creating fixtures.
	ready, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	for {
		if err = e.Kafka.Ping(ready); err == nil {
			break
		}
		select {
		case <-ready.Done():
			return nil, fmt.Errorf("lab Kafka did not become ready: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	_, err = kadm.NewClient(e.Kafka).CreateTopic(ctx, 1, replicationFactor, nil, e.Name)
	if err == nil && wire {
		err = outbox.InstallStream(ctx, e.DB, "events", e.Name)
	}
	return e, err
}

func (e *Env) Config() relay.Config {
	return relay.Config{DatabaseURL: e.URL, Brokers: []string{Broker}, Topic: e.Name,
		CDCFormat:   "legacy-json", // Keep historical JSON fault/benchmark fixtures explicit.
		Compression: "zstd", Poll: 10 * time.Millisecond, Timeout: 30 * time.Second,
		Batch: 1000, BatchBytes: 4 << 20}
}

func (e *Env) Consumer(options ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append(options, kgo.SeedBrokers(Broker), kgo.FetchMaxWait(10*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{e.Name: {0: kgo.NewOffset().AtStart()}}))...)
}

// Write records expectations independently of the trigger, in the same business TX.
func Write(ctx context.Context, db *pgx.Conn, ids []int64, sent []int64, payloads []string) error {
	return write(ctx, db, ids, sent, payloads, false)
}

func WriteOutbox(ctx context.Context, db *pgx.Conn, ids []int64, sent []int64, payloads []string) error {
	return write(ctx, db, ids, sent, payloads, true)
}

func write(ctx context.Context, db *pgx.Conn, ids []int64, sent []int64, payloads []string, wire bool) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	items := make([][]any, len(ids))
	expected := make([][]any, len(ids))
	for i, id := range ids {
		items[i] = []any{id, sent[i], payloads[i]}
		expected[i] = []any{id, sent[i], fmt.Sprintf("%x", sha256.Sum256([]byte(payloads[i])))}
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"items"}, []string{"id", "sent_ns", "payload"}, pgx.CopyFromRows(items)); err != nil {
		return err
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"expected"}, []string{"id", "sent_ns", "checksum"}, pgx.CopyFromRows(expected)); err != nil {
		return err
	}
	if wire {
		_, err = tx.Exec(ctx, `SELECT rowrelay_outbox.enqueue('events',
			('00000000-0000-0000-0000-' || lpad(to_hex(id),12,'0'))::uuid,
			convert_to(id::text,'UTF8'), int8send(id) || int8send(sent_ns) || convert_to(payload,'UTF8'))
			FROM items WHERE id=ANY($1) ORDER BY id`, ids)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (e *Env) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e.Kafka != nil {
		_, _ = kadm.NewClient(e.Kafka).DeleteTopics(ctx, e.Name)
		e.Kafka.Close()
	}
	if e.DB != nil {
		e.DB.Close(ctx)
	}
	if e.Admin != nil {
		e.Admin.Close(ctx)
	}
	if e.parent != nil {
		// This name is generated by New, never supplied by the caller.
		_, _ = e.parent.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{e.Name}.Sanitize()+" WITH (FORCE)")
		e.parent.Close(ctx)
	}
}
