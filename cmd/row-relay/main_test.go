package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sleepkqq/row-relay/internal/relay"
)

func TestFailureContextDoesNotExposeDriverMessage(t *testing.T) {
	err := errors.Join(relay.ErrMaintenance, &pgconn.PgError{Code: "57014", Message: "secret row or connection text"})
	if got := errorSummary(err); got != "maintenance (SQLSTATE 57014)" {
		t.Fatal(got)
	}
	if got := errorSummary(errors.Join(relay.ErrMaintenance, context.DeadlineExceeded)); got != "maintenance (deadline exceeded)" {
		t.Fatal(got)
	}
}

func TestFailureContextUsesAuthoredOperationLabelsOnly(t *testing.T) {
	// OperationError renders only the authored operation and, when present, the
	// SQLSTATE code; the wrapped driver text must never surface.
	err := &relay.OperationError{Operation: "Kafka publish", Err: errors.New("postgres://user:password@host:5432 secret row")}
	if got := errorSummary(err); got != "Kafka publish (operation rejected)" {
		t.Fatal(got)
	}
	withPg := &relay.OperationError{Operation: "source snapshot fetch",
		Err: &pgconn.PgError{Code: "57014", Message: "secret row or connection text"}}
	if got := errorSummary(withPg); got != "source snapshot fetch (SQLSTATE 57014)" {
		t.Fatal(got)
	}
	if got := errorSummary(fmt.Errorf("wrapped: %w", withPg)); got != "source snapshot fetch (SQLSTATE 57014)" {
		t.Fatal(got)
	}
}
