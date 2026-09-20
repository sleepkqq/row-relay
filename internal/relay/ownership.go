package relay

import (
	"context"
	"errors"
	"time"
)

const ownershipQuery = `SELECT EXISTS(SELECT 1 FROM pg_locks
	WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted
	AND mode='ExclusiveLock' AND objsubid=1
	AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
	AND classid=(($1::bigint >> 32) & 4294967295)::oid
	AND objid=($1::bigint & 4294967295)::oid)`

// acknowledge uses the original ownership connection only. It never reconnects.
// Losing that PostgreSQL session releases its lock and makes this transaction
// impossible to commit. The explicit check also rejects a manually unlocked owner.
func (r *Runner) acknowledge(ctx context.Context, batchID int64) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	var held bool
	err = tx.QueryRow(ctx, ownershipQuery, r.lockID).Scan(&held)
	if err != nil {
		return err
	}
	if !held {
		return errors.New("source ownership lost before acknowledgement")
	}
	_, err = tx.Exec(ctx, "SELECT pgque.finish_batch($1)", batchID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
