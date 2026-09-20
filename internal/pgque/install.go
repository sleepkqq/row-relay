// Package pgque installs the bundled, unmodified upstream PgQue dependency.
package pgque

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed pgque.sql
var schema string

// Install is explicit administrative setup, never a runtime migration or upgrade.
// Reserving the schema first rejects existing installations and concurrent setup.
func Install(ctx context.Context, db *pgx.Conn) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "CREATE SCHEMA pgque"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
