// Copyright 2026 sleepkqq
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

func main() {
	showVersion := flag.Bool("version", false, "print the version")
	installPgQue := flag.Bool("install-pgque", false, "install bundled PgQue in a fresh database (administrative connection required)")
	install := flag.Bool("install", false, "install capture explicitly (does not reset existing data)")
	tables := flag.String("tables", "", "comma-separated schema.table names for installation")
	topic := flag.String("topic", "", "existing output topic (CDC requires one partition)")
	cdcFormat := flag.String("cdc-format", "protobuf", "CDC wire format: protobuf (SCHEMA_REGISTRY_URL required), or explicit legacy-json fixtures")
	poll := flag.Duration("poll", 50*time.Millisecond, "poll/tick cadence")
	timeout := flag.Duration("timeout", 30*time.Second, "operation/batch timeout")
	progressInterval := flag.Duration("progress-interval", 0, "opt-in CDC closed-boundary records; requires a compatible consumer")
	batch := flag.Int("batch", 1000, "maximum Kafka chunk records")
	batchBytes := flag.Int("batch-bytes", 4<<20, "maximum Kafka chunk bytes")
	compression := flag.String("compression", "zstd", "Kafka compression: none or zstd")
	deliveryMode := flag.String("delivery-mode", "fenced", "fenced (Kafka transactions) or managed (single publisher, idempotent acks=all)")
	managedTakeover := flag.Bool("managed-takeover", false, "managed outbox failover; consumers must atomically deduplicate delivery IDs with effects")
	managedInvalidation := flag.Bool("managed-invalidation", false, "managed CDC progress/takeover for invalidation-only consumers; never materialize rows")
	stream := flag.String("outbox-stream", "", "registered business-outbox stream instead of CDC")
	config := flag.String("config", "", "JSON file with explicit CDC/outbox stream routes")
	healthAddress := flag.String("health-address", "", "optional operational probe listen address, e.g. 127.0.0.1:8080")
	flag.Parse()
	if *showVersion && flag.NArg() == 0 {
		fmt.Println("row-relay " + version)
		return
	}
	if flag.NArg() != 0 || (*config == "" && os.Getenv("DATABASE_URL") == "") {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required; positional arguments are not supported")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *config != "" && (*install || *installPgQue || *stream != "" || *tables != "" || *topic != "") {
		fail("config cannot be combined with single-source installation or routing flags")
	}
	if *installPgQue && (*install || *stream != "" || *tables != "" || *topic != "") {
		fail("install-pgque cannot be combined with source installation or routing flags")
	}
	if *install || *installPgQue {
		setupCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		db, err := pgx.Connect(setupCtx, os.Getenv("DATABASE_URL"))
		if err != nil {
			fail("connect source")
		}
		defer db.Close(context.Background())
		if *installPgQue {
			if pgque.Install(setupCtx, db) != nil {
				fail("install PgQue (existing installations are never modified)")
			}
			fmt.Println("PgQue 0.2.0 installed")
			return
		}
		if *stream != "" {
			if *tables != "" || outbox.InstallStream(setupCtx, db, *stream, *topic) != nil {
				fail("install business-outbox stream")
			}
			fmt.Println("business-outbox stream installed")
			return
		}
		var names [][2]string
		for _, name := range strings.Split(*tables, ",") {
			if *tables == "" {
				break // Source bootstrap precedes application-owned capture migrations.
			}
			parts := strings.Split(name, ".")
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				fail("tables must be schema.table pairs")
			}
			names = append(names, [2]string{parts[0], parts[1]})
		}
		if capture.Install(setupCtx, db, names) != nil {
			fail("install capture (existing sources are never reset)")
		}
		fmt.Println("capture installed")
		return
	}
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	c := relay.Config{DatabaseURL: os.Getenv("DATABASE_URL"), Brokers: brokers,
		Topic: *topic, Poll: *poll, Timeout: *timeout,
		CDCFormat: *cdcFormat, SchemaRegistryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
		Batch: *batch, BatchBytes: *batchBytes, Compression: *compression, OutboxStream: *stream,
		ProgressInterval:    *progressInterval,
		DeliveryMode:        *deliveryMode,
		ManagedTakeover:     *managedTakeover,
		ManagedInvalidation: *managedInvalidation,
		Security:            relay.KafkaSecurityFromEnv(os.Getenv)}
	if *config != "" {
		file, err := os.Open(*config)
		if err != nil {
			fail("read stream configuration")
		}
		streams, err := relay.LoadStreams(file, c, os.LookupEnv)
		file.Close()
		if err != nil {
			fail("validate stream configuration")
		}
		deadlines := make(map[string]time.Duration, len(streams))
		managed := make(map[string]bool, len(streams))
		for _, source := range streams {
			deadlines[source.Name] = source.Config.Timeout + source.Config.Poll + time.Second
			if source.Config.DeliveryMode == "managed" {
				managed[source.Name] = !source.Config.ManagedTakeover && !source.Config.ManagedInvalidation
				if source.Config.ManagedInvalidation {
					fmt.Fprintln(os.Stderr, "row-relay stream="+source.Name+": managed invalidation-only progress; late duplicate invalidations permitted; no broker fencing")
				} else if source.Config.ManagedTakeover {
					fmt.Fprintln(os.Stderr, "row-relay stream="+source.Name+": managed replay takeover; atomic consumer deduplication required; no broker fencing")
				} else {
					fmt.Fprintln(os.Stderr, "row-relay stream="+source.Name+": managed delivery; one publisher required; no broker takeover fencing")
				}
			}
		}
		status := health.New(deadlines)
		closeProbes, err := startProbes(ctx, stop, *healthAddress, status)
		if err != nil {
			fail("start health listener")
		}
		defer closeProbes()
		lastState := map[string]string{}
		err = relay.RunStreams(ctx, streams, func(name string, err error) {
			state := "active (not freshness-certified)"
			if errors.Is(err, relay.ErrOwnershipUnavailable) && managed[name] {
				status.Fail(name)
				state = "ownership conflict in managed mode; one publisher required"
			} else if errors.Is(err, relay.ErrOwnershipUnavailable) {
				status.Standby(name)
				state = "standby (source owned by another publisher)"
			} else if err != nil {
				status.Fail(name)
				state = errorSummary(err) + "; retrying without source advancement"
			} else {
				status.Success(name)
			}
			if lastState[name] != state {
				fmt.Fprintln(os.Stderr, "row-relay stream="+name+": "+state)
				lastState[name] = state
			}
		})
		if probeErr := closeProbes(); probeErr != nil {
			fail("health listener")
		}
		if err != nil {
			fail("run registered streams")
		}
		return
	}
	if err := c.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	status := health.New(map[string]time.Duration{"source": c.Timeout + c.Poll + time.Second})
	closeProbes, err := startProbes(ctx, stop, *healthAddress, status)
	if err != nil {
		fail("start health listener")
	}
	defer closeProbes()
	openCtx, cancel := context.WithTimeout(ctx, *timeout)
	runner, err := relay.Open(openCtx, c)
	cancel()
	if err != nil {
		fail("open source/publisher (check ownership, PgQue installation, and topic partitions)")
	}
	defer runner.Close()
	if c.DeliveryMode == "managed" {
		if c.ManagedTakeover {
			fmt.Fprintln(os.Stderr, "managed replay publisher started; atomic consumer deduplication required; no broker fencing")
		} else {
			fmt.Fprintln(os.Stderr, "managed publisher started; idempotent acks=all; one publisher required; no broker takeover fencing")
		}
	} else {
		fmt.Fprintln(os.Stderr, "fenced publisher started; consumers require read_committed; freshness is not certified")
	}
	err = runner.RunObserved(ctx, func() { status.Success("source") })
	if err != nil {
		status.Fail("source")
	}
	if probeErr := closeProbes(); probeErr != nil {
		fail("health listener")
	}
	if err != nil && ctx.Err() == nil {
		fail(errorSummary(err) + "; source remains replayable")
	}
}

func errorSummary(err error) string {
	stage, cause := "delivery", "operation rejected"
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

func fail(operation string) {
	// Driver/codec errors may contain credentials or entire rows.
	fmt.Fprintln(os.Stderr, "row-relay: "+operation+" failed")
	os.Exit(1)
}
