package relay

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStreamConfigExplicitRoutesAndBounds(t *testing.T) {
	base := Config{Brokers: []string{"localhost:9092"}, Compression: "zstd", Poll: time.Millisecond,
		SchemaRegistryURL: "http://registry/apis/ccompat/v7",
		Timeout:           time.Second, Batch: 1000, BatchBytes: 4 << 20}
	lookup := func(name string) (string, bool) { return "private-dsn", name == "SOURCE_DB" }
	valid := `{"streams":[{"name":"cdc","database_env":"SOURCE_DB","topic":"cdc"},
		{"name":"outbox","database_env":"SOURCE_DB","topic":"events","outbox_stream":"events"}]}`
	streams, err := LoadStreams(strings.NewReader(valid), base, lookup)
	if err != nil || len(streams) != 2 || streams[1].Config.OutboxStream != "events" {
		t.Fatal("valid mixed-source configuration rejected", err)
	}
	managed := strings.Replace(valid, `"name":"outbox"`, `"name":"outbox","delivery_mode":"managed"`, 1)
	streams, err = LoadStreams(strings.NewReader(managed), base, lookup)
	if err != nil || streams[1].Config.DeliveryMode != "managed" || streams[0].Config.DeliveryMode != "" {
		t.Fatal("explicit managed route or fenced default changed", err)
	}
	takeover := strings.Replace(managed, `"delivery_mode":"managed"`, `"delivery_mode":"managed","managed_takeover":true`, 1)
	streams, err = LoadStreams(strings.NewReader(takeover), base, lookup)
	if err != nil || !streams[1].Config.ManagedTakeover || streams[0].Config.ManagedTakeover {
		t.Fatal("explicit outbox replay takeover rejected", err)
	}
	for _, input := range []string{
		strings.Replace(valid, "SOURCE_DB", "MISSING", 1),
		strings.Replace(valid, `"outbox_stream":"events"`, `"outbox_stream":""`, 1),
		strings.Replace(valid, `"topic":"events"`, `"topic":"events","batch_bytes":67108864`, 1),
		strings.Replace(valid, `"topic":"events"`, `"topic":"events","password":"secret"`, 1),
		strings.Replace(valid, `"topic":"events"`, `"topic":"events","mode":"pending"`, 1),
		strings.Replace(valid, `"topic":"events"`, `"topic":"events","delivery_mode":"fallback"`, 1),
		strings.Replace(valid, `"topic":"cdc"`, `"topic":"cdc","delivery_mode":"managed","progress_interval":"1s"`, 1),
		strings.Replace(valid, `"topic":"cdc"`, `"topic":"cdc","delivery_mode":"managed","managed_takeover":true`, 1),
		strings.Replace(valid, `"topic":"events"`, `"topic":"events","managed_takeover":true`, 1),
		valid + `{}`, strings.Repeat(" ", 65537),
	} {
		if _, err = LoadStreams(strings.NewReader(input), base, lookup); err == nil || strings.Contains(err.Error(), "private-dsn") {
			t.Fatal("unsafe configuration accepted or secret leaked", err)
		}
	}
}

func TestManagedInvalidationRequiresExplicitClosedProgressAndSchemaRoutes(t *testing.T) {
	base := Config{Brokers: []string{"localhost:9092"}, Compression: "none", Poll: time.Millisecond,
		SchemaRegistryURL: "http://registry/apis/ccompat/v7",
		Timeout:           time.Second, Batch: 1000, BatchBytes: 4 << 20}
	lookup := func(string) (string, bool) { return "private-dsn", true }
	valid := `{"streams":[{"name":"cache","database_env":"DB","delivery_mode":"managed",
		"managed_invalidation":true,"progress_interval":"1s","schema_topics":{"social":"social.cache-cdc","chat":"chat.cache-cdc"}}]}`
	streams, err := LoadStreams(strings.NewReader(valid), base, lookup)
	if err != nil || len(streams) != 1 || !streams[0].Config.ManagedInvalidation || len(streams[0].Config.topics()) != 2 {
		t.Fatal("explicit invalidation routes rejected", err)
	}
	for _, input := range []string{
		strings.Replace(valid, `"managed_invalidation":true`, `"managed_invalidation":false`, 1),
		strings.Replace(valid, `"progress_interval":"1s"`, `"progress_interval":"0s"`, 1),
		strings.Replace(valid, `"delivery_mode":"managed"`, `"delivery_mode":"fenced"`, 1),
		strings.Replace(valid, `"name":"cache"`, `"name":"cache","outbox_stream":"events"`, 1),
		strings.Replace(valid, `"chat.cache-cdc"`, `"social.cache-cdc"`, 1),
	} {
		if _, err = LoadStreams(strings.NewReader(input), base, lookup); err == nil {
			t.Fatal("ambiguous invalidation contract accepted")
		}
	}
}

func TestStreamConfigBounds(t *testing.T) {
	base := Config{Brokers: []string{"localhost:9092"}, Compression: "none", CDCFormat: "legacy-json",
		Timeout: time.Second, Poll: time.Millisecond, Batch: 1000, BatchBytes: 2 << 20}
	lookup := func(db string) (string, bool) { return "dsn-" + db, true }
	config := func(count, batchBytes int) string {
		streams := make([]string, 0, count)
		for i := 0; i < count; i++ {
			streams = append(streams, fmt.Sprintf(
				`{"name":"s%d","database_env":"DB%d","topic":"cdc","cdc_format":"legacy-json","batch_bytes":%d}`, i, i, batchBytes))
		}
		return `{"streams":[` + strings.Join(streams, ",") + `]}`
	}
	for _, tc := range []struct {
		name  string
		input string
		want  int
	}{
		{"20x2MiB", config(20, 2<<20), 20},
		{"32x2MiB", config(32, 2<<20), 32},
	} {
		streams, err := LoadStreams(strings.NewReader(tc.input), base, lookup)
		if err != nil || len(streams) != tc.want {
			t.Fatalf("%s rejected: len=%d err=%v", tc.name, len(streams), err)
		}
	}
	// 64 MiB + 1 byte: each route is within 1..64 MiB, the sum is not.
	over := `{"streams":[{"name":"a","database_env":"DB","topic":"cdc","cdc_format":"legacy-json","batch_bytes":66060288},` +
		`{"name":"b","database_env":"DB2","topic":"cdc","cdc_format":"legacy-json","batch_bytes":1048577}]}`
	for _, input := range []string{config(0, 2<<20), config(33, 1<<20), over} {
		if _, err := LoadStreams(strings.NewReader(input), base, lookup); err == nil {
			t.Fatalf("out-of-range stream configuration accepted: %s", input)
		}
	}
}

func TestRunStreamsActivityBounds(t *testing.T) {
	newStream := func(batchBytes int) Stream {
		return Stream{Name: "s", Config: Config{DatabaseURL: "postgres://relay:pw@127.0.0.1:5432/db",
			Brokers: []string{"localhost:9092"}, Compression: "none", Topic: "cdc", CDCFormat: "legacy-json",
			Timeout: time.Second, Poll: time.Millisecond, Batch: 1000, BatchBytes: batchBytes}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A cancelled context never opens transport, so a missing bound guard fails
	// the assertion instead of retrying until the test times out.
	for _, count := range []int{20, 32} {
		streams := make([]Stream, count)
		for i := range streams {
			streams[i] = newStream(2 << 20)
		}
		if err := RunStreamsActivity(ctx, streams, nil, nil); err != nil {
			t.Fatalf("%d streams rejected: %v", count, err)
		}
	}
	tooMany := make([]Stream, 33)
	for i := range tooMany {
		tooMany[i] = newStream(1 << 20)
	}
	tooLarge := make([]Stream, 32)
	for i := range tooLarge {
		tooLarge[i] = newStream(2 << 20)
	}
	tooLarge[31] = newStream((2 << 20) + 1)
	for name, streams := range map[string][]Stream{"33 streams": tooMany, "over budget": tooLarge} {
		if err := RunStreamsActivity(ctx, streams, nil, nil); err == nil {
			t.Fatalf("%s accepted at runtime", name)
		}
	}
}
