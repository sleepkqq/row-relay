package capture

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var Schema string

// Install owns one source per database. Existing installations are never reset.
// tables are schema/table pairs, quoted as identifiers rather than interpolated SQL.
func Install(ctx context.Context, db *pgx.Conn, tables [][2]string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, Schema); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO rowrelay.source(queue_mode) VALUES ('pgque')"); err != nil {
		return err
	}
	for _, sql := range []string{
		"SELECT pgque.create_queue('rowrelay')",
		"SELECT pgque.register_consumer('rowrelay', 'rowrelay')",
		"SELECT pgque.set_queue_config('rowrelay', 'ticker_max_count', '1')",
		"SELECT pgque.set_queue_config('rowrelay', 'ticker_max_lag', '10 milliseconds')",
		"SELECT pgque.set_queue_config('rowrelay', 'ticker_idle_period', '100 milliseconds')",
	} {
		if _, err = tx.Exec(ctx, sql); err != nil {
			return err
		}
	}
	for _, table := range tables {
		if _, err = tx.Exec(ctx, "SELECT rowrelay.install_capture($1,$2)", table[0], []string{table[1]}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
