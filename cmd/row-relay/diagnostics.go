// Copyright 2026 sleepkqq
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"log/slog"
	"time"

	"github.com/sleepkqq/row-relay/internal/health"
)

// Stream worker phases reported by the relay callback.
const (
	phaseActive   = "active"
	phaseStandby  = "standby"
	phaseConflict = "ownership-conflict"
	phaseFailed   = "failed"
)

// diagnostics emits timestamped, structured lifecycle and repeated-error
// messages to stderr. It never writes DSNs, broker addresses, credentials, row
// data, or raw driver errors; failure reasons pass through errorSummary.
type diagnostics struct {
	log          *slog.Logger
	summaryEvery time.Duration
	now          func() time.Time
	states       map[string]*streamStatus
}

type streamStatus struct {
	phase       string
	failures    uint64
	firstFailed time.Time
	lastSummary time.Time
}

func newDiagnostics(w io.Writer) *diagnostics {
	return &diagnostics{
		log:          slog.New(slog.NewTextHandler(w, nil)),
		summaryEvery: time.Minute,
		now:          time.Now,
		states:       map[string]*streamStatus{},
	}
}

// startup records the effective, bounded runtime shape without endpoints.
func (d *diagnostics) startup(streams, totalBatchBytes int, mode, poll, timeout string) {
	d.log.Info("relay starting", "version", version, "streams", streams,
		"total_batch_bytes", totalBatchBytes, "mode", mode, "poll", poll, "timeout", timeout)
}

// observe records one serialized stream callback, updating operational health
// and suppressing repeated identical states. Repeated degraded states (failure
// or managed ownership conflict) are summarized at a bounded cadence; there is
// no per-step or per-record logging.
func (d *diagnostics) observe(name, phase string, err error, status *health.Health) {
	switch phase {
	case phaseActive:
		status.Success(name)
	case phaseStandby:
		status.Standby(name)
	default:
		status.Fail(name)
	}
	now := d.now()
	st := d.states[name]
	if st == nil {
		st = &streamStatus{}
		d.states[name] = st
	}
	degraded := phase == phaseFailed || phase == phaseConflict
	if st.phase == phase {
		if degraded {
			st.failures++
			if now.Sub(st.lastSummary) >= d.summaryEvery {
				d.log.Warn("stream still failing", "stream", name, "state", phase, "failures", st.failures,
					"since", now.Sub(st.firstFailed).Round(time.Second).String(), "reason", errorSummary(err))
				st.lastSummary = now
			}
		}
		return
	}
	previous := st.phase
	switch phase {
	case phaseFailed:
		st.firstFailed, st.failures, st.lastSummary = now, 1, now
		d.log.Warn("stream failure", "stream", name, "reason", errorSummary(err),
			"action", "retrying without source advancement")
	case phaseConflict:
		st.firstFailed, st.failures, st.lastSummary = now, 1, now
		d.log.Warn("stream ownership conflict", "stream", name,
			"reason", "managed mode requires one publisher")
	case phaseStandby:
		if previous == phaseFailed || previous == phaseConflict {
			d.log.Info("stream recovered", "stream", name, "state", phaseStandby,
				"outage", now.Sub(st.firstFailed).Round(time.Second).String(), "failures", st.failures)
			st.firstFailed, st.failures = time.Time{}, 0
		} else {
			d.log.Info("stream standby", "stream", name,
				"reason", "source owned by another publisher")
		}
	case phaseActive:
		switch previous {
		case phaseFailed, phaseConflict:
			d.log.Info("stream recovered", "stream", name, "state", phaseActive,
				"outage", now.Sub(st.firstFailed).Round(time.Second).String(), "failures", st.failures)
			st.firstFailed, st.failures = time.Time{}, 0
		case phaseStandby:
			d.log.Info("stream active", "stream", name, "ownership", "acquired")
		default:
			d.log.Info("stream active", "stream", name)
		}
	}
	st.phase = phase
}

// shutdown records the terminal per-stream failure counts.
func (d *diagnostics) shutdown() {
	for name, st := range d.states {
		d.log.Info("stream stopped", "stream", name, "state", st.phase, "failures", st.failures)
	}
}

func modeName(mode string) string {
	if mode == "" {
		return "fenced"
	}
	return mode
}

func uniformMode(modes map[string]bool) string {
	if len(modes) == 1 {
		for mode := range modes {
			return mode
		}
	}
	return "mixed"
}

func uniformDuration(uniform bool, value time.Duration) string {
	if !uniform {
		return "varies"
	}
	return value.String()
}
