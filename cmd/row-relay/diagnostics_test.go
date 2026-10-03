// Copyright 2026 sleepkqq
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sleepkqq/row-relay/internal/health"
	"github.com/sleepkqq/row-relay/internal/relay"
)

func TestDiagnosticsSuppressRepeatedFailuresAndRecover(t *testing.T) {
	var output bytes.Buffer
	diag := newDiagnostics(&output)
	diag.summaryEvery = time.Minute
	now := time.Unix(0, 0)
	diag.now = func() time.Time { return now }
	status := health.New(map[string]time.Duration{"s": time.Second})
	pgError := &pgconn.PgError{Code: "57014", Message: "secret row or connection text"}

	diag.observe("s", phaseFailed, errors.Join(relay.ErrMaintenance, pgError), status)
	if lines := strings.Count(output.String(), "\n"); lines != 1 {
		t.Fatalf("expected one transition line, got %d: %q", lines, output.String())
	}
	for i := 0; i < 10; i++ {
		diag.observe("s", phaseFailed, relay.ErrMaintenance, status)
	}
	if lines := strings.Count(output.String(), "\n"); lines != 1 {
		t.Fatalf("repeated failures were not suppressed: %q", output.String())
	}
	now = now.Add(time.Minute)
	diag.observe("s", phaseFailed, relay.ErrMaintenance, status)
	if !strings.Contains(output.String(), "still failing") || !strings.Contains(output.String(), "failures=12") {
		t.Fatalf("missing bounded summary: %q", output.String())
	}
	now = now.Add(time.Second)
	diag.observe("s", phaseActive, nil, status)
	if !strings.Contains(output.String(), "stream recovered") {
		t.Fatalf("missing recovery message: %q", output.String())
	}
	if !strings.Contains(output.String(), "57014") {
		t.Fatalf("expected SQLSTATE-only reason: %q", output.String())
	}
	if strings.Contains(output.String(), "secret") {
		t.Fatalf("diagnostics leaked driver message: %q", output.String())
	}
}

func TestDiagnosticsStartupIsStructuredAndBounded(t *testing.T) {
	var output bytes.Buffer
	diag := newDiagnostics(&output)
	diag.startup(3, 12<<20, "mixed", "50ms", "30s")
	line := output.String()
	for _, want := range []string{"time=", "level=INFO", "relay starting", "streams=3", "total_batch_bytes=12582912", "mode=mixed"} {
		if !strings.Contains(line, want) {
			t.Fatalf("startup line missing %q: %q", want, line)
		}
	}
}

func TestDiagnosticsSummarisesOwnershipConflictAndRecoversToStandby(t *testing.T) {
	var output bytes.Buffer
	diag := newDiagnostics(&output)
	diag.summaryEvery = time.Minute
	now := time.Unix(0, 0)
	diag.now = func() time.Time { return now }
	status := health.New(map[string]time.Duration{"s": time.Second})

	diag.observe("s", phaseConflict, relay.ErrOwnershipUnavailable, status)
	if !strings.Contains(output.String(), "ownership conflict") {
		t.Fatalf("missing conflict transition: %q", output.String())
	}
	for i := 0; i < 5; i++ {
		diag.observe("s", phaseConflict, relay.ErrOwnershipUnavailable, status)
	}
	if lines := strings.Count(output.String(), "\n"); lines != 1 {
		t.Fatalf("repeated conflicts were not suppressed: %q", output.String())
	}
	now = now.Add(time.Minute)
	diag.observe("s", phaseConflict, relay.ErrOwnershipUnavailable, status)
	if !strings.Contains(output.String(), "still failing") ||
		!strings.Contains(output.String(), "state=ownership-conflict") ||
		!strings.Contains(output.String(), "failures=7") {
		t.Fatalf("missing bounded conflict summary: %q", output.String())
	}
	now = now.Add(time.Second)
	diag.observe("s", phaseStandby, relay.ErrOwnershipUnavailable, status)
	if !strings.Contains(output.String(), "stream recovered") || !strings.Contains(output.String(), "state=standby") {
		t.Fatalf("missing standby recovery: %q", output.String())
	}
	// Counters reset on recovery: the next conflict transitions fresh.
	now = now.Add(time.Second)
	diag.observe("s", phaseConflict, relay.ErrOwnershipUnavailable, status)
	if !strings.Contains(output.String(), "stream ownership conflict") {
		t.Fatalf("missing post-recovery conflict: %q", output.String())
	}
}
