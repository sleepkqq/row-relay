//go:build integration

package relay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
)

func TestApplicationAndPublisherRolesAreSeparate(t *testing.T) {
	for _, mode := range []string{"pgque", "outbox-pgque"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			e, err := lab.New(ctx, mode)
			if err != nil {
				t.Fatal(err)
			}
			writer, publisher := e.Name+"_writer", e.Name+"_publisher"
			cleanupConfig := e.Admin.Config().Copy()
			cleanupConfig.Database = "rowrelay"
			defer func() {
				e.Close() // Drop this fixture's object grants before dropping its roles.
				cleanup, finish := context.WithTimeout(context.Background(), 10*time.Second)
				defer finish()
				admin, err := pgx.ConnectConfig(cleanup, cleanupConfig)
				if err != nil {
					t.Error(err)
					return
				}
				defer admin.Close(cleanup)
				for _, role := range []string{writer, publisher} {
					if _, err := admin.Exec(cleanup, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize()); err != nil {
						t.Error(err)
					}
				}
			}()
			for _, role := range []string{writer, publisher} {
				if _, err := e.Admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+
					" LOGIN PASSWORD 'lab' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"); err != nil {
					t.Fatal(err)
				}
			}
			w, p := pgx.Identifier{writer}.Sanitize(), pgx.Identifier{publisher}.Sanitize()
			// Managed installations can revoke PUBLIC execution on invoker helpers.
			if _, err := e.Admin.Exec(ctx, "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA pgque FROM PUBLIC"); err != nil {
				t.Fatal(err)
			}
			schema, source := "rowrelay", "source"
			if mode == "outbox-pgque" {
				schema, source = "rowrelay_outbox", "stream"
				if _, err := e.Admin.Exec(ctx, "GRANT USAGE ON SCHEMA rowrelay_outbox TO "+w+
					"; GRANT EXECUTE ON FUNCTION rowrelay_outbox.enqueue(text,uuid,bytea,bytea,jsonb) TO "+w); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := e.Admin.Exec(ctx, "GRANT SELECT,INSERT,UPDATE,DELETE ON items TO "+w+
				"; GRANT USAGE ON SCHEMA "+schema+" TO "+p+
				"; GRANT SELECT ON "+schema+"."+source+" TO "+p+
				"; GRANT pgque_reader TO "+p+
				"; GRANT EXECUTE ON FUNCTION pgque.batch_event_sql(bigint),pgque.batch_event_tables(bigint),pgque.quote_fqname(text),pgque.ticker(text),pgque.maint(),pgque.maint_rotate_tables_step2() TO "+p+
				"; GRANT UPDATE(queue_switch_step2) ON pgque.queue TO "+p); err != nil {
				t.Fatal(err)
			}
			connect := func(role string) *pgx.Conn {
				t.Helper()
				config := e.DB.Config().Copy()
				config.User, config.Password = role, "lab"
				conn, err := pgx.ConnectConfig(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { conn.Close(context.Background()) })
				return conn
			}
			app, worker := connect(writer), connect(publisher)
			denied := func(conn *pgx.Conn, sql string) {
				t.Helper()
				_, err := conn.Exec(ctx, sql)
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
					t.Fatalf("expected permission denial, got %v", err)
				}
			}
			denied(app, "SELECT pgque.finish_batch(1)")
			denied(app, "UPDATE "+schema+"."+source+" SET epoch=gen_random_uuid()")
			denied(worker, "INSERT INTO items VALUES(99,1,'forbidden',NULL)")
			denied(worker, "UPDATE "+schema+"."+source+" SET epoch=gen_random_uuid()")
			denied(worker, "SELECT pgque.drop_queue('rowrelay',true)")
			if _, err := app.Exec(ctx, "INSERT INTO items VALUES(1,1,'role-separated',NULL)"); err != nil {
				t.Fatal(err)
			}
			queue := "rowrelay"
			if mode == "outbox-pgque" {
				if _, err := app.Exec(ctx, `SELECT rowrelay_outbox.enqueue('events',
					'00000000-0000-0000-0000-000000000001','key'::bytea,'value'::bytea)`); err != nil {
					t.Fatal(err)
				}
				if err := e.DB.QueryRow(ctx, "SELECT 'rowrelay_outbox.' || epoch FROM rowrelay_outbox.stream").Scan(&queue); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := e.DB.Exec(ctx, "SELECT pgque.force_next_tick($1)", queue); err != nil {
				t.Fatal(err)
			}
			if _, err := worker.Exec(ctx, "SELECT pgque.ticker($1)", queue); err != nil {
				t.Fatal(err)
			}
			for _, sql := range []string{"SELECT pgque.maint()", "SELECT pgque.maint_rotate_tables_step2()"} {
				if _, err := worker.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			config := e.Config()
			config.DatabaseURL = worker.Config().ConnString()
			if mode == "outbox-pgque" {
				config.OutboxStream = "events"
			} else {
				config.ProgressInterval = time.Nanosecond
			}
			r, err := relay.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if n, _, err := r.Step(ctx); err != nil || n != 1 {
				t.Fatalf("least-privilege delivery: count=%d, error=%v", n, err)
			}
		})
	}
}
