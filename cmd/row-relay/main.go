// Copyright 2026 sleepkqq
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sleepkqq/row-relay/internal/capture"
	"github.com/sleepkqq/row-relay/internal/health"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/sleepkqq/row-relay/internal/pgque"
	"github.com/sleepkqq/row-relay/internal/relay"
)

var version = "dev"

// errConfigFile keeps the configuration path out of diagnostics: paths may be
// sensitive, so file errors stay sanitized while LoadStreams' locally authored
// validation errors remain printable.
var errConfigFile = errors.New("cannot read stream configuration")

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.LookupEnv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("row-relay", flag.ContinueOnError)
	// Parsing must not echo adversarial flag values, so the standard parser
	// writes to io.Discard and failures are reported as fixed text.
	flags.SetOutput(io.Discard)
	showVersion := flags.Bool("version", false, "print the version")
	checkConfig := flags.Bool("check-config", false, "validate the effective configuration and exit without connecting to PostgreSQL, Kafka, or the schema registry")
	installPgQue := flags.Bool("install-pgque", false, "install bundled PgQue in a fresh database (administrative connection required)")
	install := flags.Bool("install", false, "install capture explicitly (does not reset existing data)")
	tables := flags.String("tables", "", "comma-separated schema.table names for installation")
	topic := flags.String("topic", "", "existing output topic (CDC requires one partition)")
	cdcFormat := flags.String("cdc-format", "protobuf", "CDC wire format: protobuf (SCHEMA_REGISTRY_URL required), or explicit legacy-json fixtures")
	poll := flags.Duration("poll", 50*time.Millisecond, "poll/tick cadence")
	timeout := flags.Duration("timeout", 30*time.Second, "operation/batch timeout")
	progressInterval := flags.Duration("progress-interval", 0, "opt-in CDC closed-boundary records; requires a compatible consumer")
	batch := flags.Int("batch", 1000, "maximum Kafka chunk records")
	batchBytes := flags.Int("batch-bytes", 4<<20, "maximum Kafka chunk bytes")
	compression := flags.String("compression", "zstd", "Kafka compression: none or zstd")
	deliveryMode := flags.String("delivery-mode", "fenced", "fenced (Kafka transactions) or managed (single publisher, idempotent acks=all)")
	managedTakeover := flags.Bool("managed-takeover", false, "managed outbox failover; consumers must atomically deduplicate delivery IDs with effects")
	managedInvalidation := flags.Bool("managed-invalidation", false, "managed CDC progress/takeover for invalidation-only consumers; never materialize rows")
	stream := flags.String("outbox-stream", "", "registered business-outbox stream instead of CDC")
	config := flags.String("config", "", "JSON file with explicit CDC/outbox stream routes")
	healthAddress := flags.String("health-address", "", "optional operational probe listen address, e.g. 127.0.0.1:8080")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			// Keep --help functional without echoing argument values.
			flags.SetOutput(stdout)
			flags.Usage()
			return 0
		}
		fmt.Fprintln(stderr, "row-relay: invalid command-line arguments")
		return 2
	}

	if *showVersion && *checkConfig {
		fmt.Fprintln(stderr, "row-relay: --check-config cannot be combined with --version")
		return 2
	}
	if *showVersion && flags.NArg() == 0 {
		fmt.Fprintln(stdout, "row-relay "+version)
		return 0
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "row-relay: positional arguments are not supported")
		return 2
	}
	if *checkConfig && (*install || *installPgQue) {
		fmt.Fprintln(stderr, "row-relay: --check-config cannot be combined with installation flags")
		return 2
	}
	if *config == "" && getenv("DATABASE_URL") == "" && !*checkConfig {
		fmt.Fprintln(stderr, "DATABASE_URL is required; positional arguments are not supported")
		return 2
	}
	if *config != "" && (*install || *installPgQue || *stream != "" || *tables != "" || *topic != "") {
		return fail(stderr, "config cannot be combined with single-source installation or routing flags")
	}
	if *installPgQue && (*install || *stream != "" || *tables != "" || *topic != "") {
		return fail(stderr, "install-pgque cannot be combined with source installation or routing flags")
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	c := relay.Config{DatabaseURL: getenv("DATABASE_URL"), Brokers: brokers,
		Topic: *topic, Poll: *poll, Timeout: *timeout,
		CDCFormat: *cdcFormat, SchemaRegistryURL: getenv("SCHEMA_REGISTRY_URL"),
		Batch: *batch, BatchBytes: *batchBytes, Compression: *compression, OutboxStream: *stream,
		ProgressInterval:    *progressInterval,
		DeliveryMode:        *deliveryMode,
		ManagedTakeover:     *managedTakeover,
		ManagedInvalidation: *managedInvalidation,
		Security:            relay.KafkaSecurityFromEnv(getenv)}

	// --check-config validates the effective configuration with no connection,
	// installer, probe listener, or goroutine.
	if *checkConfig {
		return checkEffectiveConfig(stdout, stderr, lookup, c, *config)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *install || *installPgQue {
		setupCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		db, err := pgx.Connect(setupCtx, getenv("DATABASE_URL"))
		if err != nil {
			return failReason(stderr, "connect source", err)
		}
		defer db.Close(context.Background())
		if *installPgQue {
			if err := pgque.Install(setupCtx, db); err != nil {
				return failReason(stderr, "install PgQue (existing installations are never modified)", err)
			}
			fmt.Fprintln(stdout, "PgQue 0.2.0 installed")
			return 0
		}
		if *stream != "" {
			if *tables != "" {
				return fail(stderr, "install business-outbox stream")
			}
			if err := outbox.InstallStream(setupCtx, db, *stream, *topic); err != nil {
				return failReason(stderr, "install business-outbox stream", err)
			}
			fmt.Fprintln(stdout, "business-outbox stream installed")
			return 0
		}
		var names [][2]string
		for _, name := range strings.Split(*tables, ",") {
			if *tables == "" {
				break // Source bootstrap precedes application-owned capture migrations.
			}
			parts := strings.Split(name, ".")
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return fail(stderr, "tables must be schema.table pairs")
			}
			names = append(names, [2]string{parts[0], parts[1]})
		}
		if err := capture.Install(setupCtx, db, names); err != nil {
			return failReason(stderr, "install capture (existing sources are never reset)", err)
		}
		fmt.Fprintln(stdout, "capture installed")
		return 0
	}
	if *config != "" {
		streams, err := loadStreams(*config, c, lookup)
		if err != nil {
			return configFail(stderr, err)
		}
		deadlines := make(map[string]time.Duration, len(streams))
		managed := make(map[string]bool, len(streams))
		modes := map[string]bool{}
		pollBase, timeoutBase := streams[0].Config.Poll, streams[0].Config.Timeout
		uniformPoll, uniformTimeout := true, true
		totalBytes := 0
		for _, source := range streams {
			deadlines[source.Name] = source.Config.Timeout + source.Config.Poll + time.Second
			modes[modeName(source.Config.DeliveryMode)] = true
			totalBytes += source.Config.BatchBytes
			if source.Config.Poll != pollBase {
				uniformPoll = false
			}
			if source.Config.Timeout != timeoutBase {
				uniformTimeout = false
			}
			if source.Config.DeliveryMode == "managed" {
				managed[source.Name] = !source.Config.ManagedTakeover && !source.Config.ManagedInvalidation
			}
		}
		diag := newDiagnostics(stderr)
		diag.startup(len(streams), totalBytes, uniformMode(modes),
			uniformDuration(uniformPoll, pollBase), uniformDuration(uniformTimeout, timeoutBase))
		for _, source := range streams {
			if source.Config.DeliveryMode != "managed" {
				continue
			}
			switch {
			case source.Config.ManagedInvalidation:
				diag.log.Warn("managed invalidation-only progress", "stream", source.Name,
					"note", "late duplicate invalidations permitted; no broker fencing")
			case source.Config.ManagedTakeover:
				diag.log.Warn("managed replay takeover", "stream", source.Name,
					"note", "atomic consumer deduplication required; no broker fencing")
			default:
				diag.log.Warn("managed delivery", "stream", source.Name,
					"note", "one publisher required; no broker takeover fencing")
			}
		}
		status := health.New(deadlines)
		closeProbes, err := startProbes(ctx, stop, *healthAddress, status)
		if err != nil {
			return fail(stderr, "start health listener")
		}
		defer closeProbes()
		err = relay.RunStreamsActivity(ctx, streams, func(name string, err error) {
			phase := phaseActive
			switch {
			case errors.Is(err, relay.ErrOwnershipUnavailable) && managed[name]:
				phase = phaseConflict
			case errors.Is(err, relay.ErrOwnershipUnavailable):
				phase = phaseStandby
			case err != nil:
				phase = phaseFailed
			}
			diag.observe(name, phase, err, status)
		}, status.Activity)
		diag.shutdown()
		if probeErr := closeProbes(); probeErr != nil {
			return fail(stderr, "health listener")
		}
		if err != nil {
			return fail(stderr, "run registered streams")
		}
		return 0
	}
	if err := c.Validate(); err != nil {
		fmt.Fprintln(stderr, "row-relay: invalid configuration: "+err.Error())
		return 2
	}
	diag := newDiagnostics(stderr)
	diag.startup(1, c.BatchBytes, modeName(c.DeliveryMode), c.Poll.String(), c.Timeout.String())
	status := health.New(map[string]time.Duration{"source": c.Timeout + c.Poll + time.Second})
	closeProbes, err := startProbes(ctx, stop, *healthAddress, status)
	if err != nil {
		return fail(stderr, "start health listener")
	}
	defer closeProbes()
	openCtx, cancel := context.WithTimeout(ctx, *timeout)
	runner, err := relay.Open(openCtx, c)
	cancel()
	if err != nil {
		diag.observe("source", phaseFailed, err, status)
		diag.shutdown()
		return failReason(stderr, "open source/publisher (check ownership, PgQue installation, and topic partitions)", err)
	}
	defer runner.Close()
	if c.DeliveryMode == "managed" {
		if c.ManagedTakeover {
			diag.log.Warn("managed replay publisher started",
				"note", "atomic consumer deduplication required; no broker fencing")
		} else {
			diag.log.Warn("managed publisher started",
				"note", "idempotent acks=all; one publisher required; no broker takeover fencing")
		}
	} else {
		diag.log.Info("fenced publisher started",
			"note", "consumers require read_committed; freshness is not certified")
	}
	err = runner.RunObservedActivity(ctx, func() { diag.observe("source", phaseActive, nil, status) }, func() { status.Activity("source") })
	if err != nil {
		diag.observe("source", phaseFailed, err, status)
	}
	diag.shutdown()
	if probeErr := closeProbes(); probeErr != nil {
		return fail(stderr, "health listener")
	}
	if err != nil && ctx.Err() == nil {
		return fail(stderr, errorSummary(err)+"; source remains replayable")
	}
	return 0
}

// checkEffectiveConfig validates a single-source or strict multi-stream
// configuration and prints a bounded, credential-free effective summary. It
// performs no I/O beyond reading the explicit configuration file.
func checkEffectiveConfig(stdout, stderr io.Writer, lookup func(string) (string, bool), c relay.Config, configPath string) int {
	count, totalBytes := 1, c.BatchBytes
	mode, poll, timeout := modeName(c.DeliveryMode), c.Poll.String(), c.Timeout.String()
	if configPath == "" {
		if err := c.Validate(); err != nil {
			fmt.Fprintln(stderr, "row-relay: invalid configuration: "+err.Error())
			return 2
		}
	} else {
		streams, err := loadStreams(configPath, c, lookup)
		if err != nil {
			return configFail(stderr, err)
		}
		modes := map[string]bool{}
		totalBytes = 0
		pollBase, timeoutBase := streams[0].Config.Poll, streams[0].Config.Timeout
		uniformPoll, uniformTimeout := true, true
		for _, source := range streams {
			totalBytes += source.Config.BatchBytes
			modes[modeName(source.Config.DeliveryMode)] = true
			if source.Config.Poll != pollBase {
				uniformPoll = false
			}
			if source.Config.Timeout != timeoutBase {
				uniformTimeout = false
			}
		}
		count, mode = len(streams), uniformMode(modes)
		poll, timeout = uniformDuration(uniformPoll, pollBase), uniformDuration(uniformTimeout, timeoutBase)
	}
	fmt.Fprintf(stdout, "row-relay check-config version=%s count=%d total_batch_bytes=%d mode=%s poll=%s timeout=%s\n",
		version, count, totalBytes, mode, poll, timeout)
	return 0
}

// loadStreams reads the explicit configuration file. File errors are replaced
// by a path-free sentinel; LoadStreams and Config.Validate only emit locally
// authored safe constants, which remain printable.
func loadStreams(path string, c relay.Config, lookup func(string) (string, bool)) ([]relay.Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errConfigFile
	}
	defer file.Close()
	return relay.LoadStreams(file, c, lookup)
}

func configFail(stderr io.Writer, err error) int {
	if errors.Is(err, errConfigFile) {
		fmt.Fprintln(stderr, "row-relay: cannot read stream configuration")
		return 2
	}
	fmt.Fprintln(stderr, "row-relay: invalid stream configuration: "+err.Error())
	return 2
}

func errorSummary(err error) string {
	stage, cause := "delivery", "operation rejected"
	// relay.OperationError.Operation is an authored constant label; the wrapped
	// driver/row cause is never rendered.
	var operationError *relay.OperationError
	if errors.As(err, &operationError) {
		stage = operationError.Operation
	}
	if errors.Is(err, relay.ErrMaintenance) {
		stage = "maintenance"
	}
	var pgError *pgconn.PgError
	if errors.Is(err, context.DeadlineExceeded) {
		cause = "deadline exceeded"
	} else if errors.As(err, &pgError) {
		cause = "SQLSTATE " + pgError.Code
	}
	return stage + " (" + cause + ")"
}

func fail(stderr io.Writer, operation string) int {
	// Driver/codec errors may contain credentials or entire rows.
	fmt.Fprintln(stderr, "row-relay: "+operation+" failed")
	return 1
}

// failReason prefixes a fixed operation with the safe classified reason; the
// raw error is never rendered.
func failReason(stderr io.Writer, operation string, err error) int {
	fmt.Fprintln(stderr, "row-relay: "+operation+" failed: "+errorSummary(err))
	return 1
}
