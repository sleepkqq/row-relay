package main

import (
	"context"
	"errors"
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
