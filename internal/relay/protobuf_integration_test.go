//go:build integration && protobuf

package relay_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPreparedProtobufWithRealRegistryAndJVMConsumer(t *testing.T) {
	root, err := filepath.Abs("../../interop/jvm")
	if err != nil {
		t.Fatal(err)
	}
	dependencies, err := os.ReadFile(filepath.Join(root, "target/classpath.txt"))
	if err != nil {
		t.Fatal("run make interop-build first:", err)
	}
	classpath := filepath.Join(root, "target/classes") + string(os.PathListSeparator) + strings.TrimSpace(string(dependencies))
	ready, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	httpClient := &http.Client{Timeout: 2 * time.Second}
	for {
		request, err := http.NewRequestWithContext(ready, http.MethodGet, "http://127.0.0.1:28080/apis/registry/v3/system/info", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := httpClient.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-ready.Done():
			t.Fatal("local Registry did not become ready; run make interop-up")
		case <-time.After(200 * time.Millisecond):
		}
	}
	for _, headerMode := range []bool{false, true} {
		t.Run("schema-id-in-headers="+strconv.FormatBool(headerMode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			e, err := lab.New(ctx, "outbox-pgque")
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			exchange := filepath.Join(t.TempDir(), "frames.json")
			jvm := func(phase string) {
				t.Helper()
				command := exec.CommandContext(ctx, "java", "-Xmx128m", "-cp", classpath,
					"rowrelay.fixture.InteropMain", phase, e.Name, strconv.FormatBool(headerMode), exchange)
				if log, err := command.CombinedOutput(); err != nil {
					t.Fatalf("JVM %s: %v\n%s", phase, err, log)
				}
			}
			jvm("prepare")
			data, err := os.ReadFile(exchange)
			if err != nil {
				t.Fatal(err)
			}
			var frames []outbox.Message
			if err = json.Unmarshal(data, &frames); err != nil || len(frames) != 3 {
				t.Fatal("invalid official serializer fixture", err)
			}
			tx, err := e.DB.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			for i, frame := range frames {
				if _, err = tx.Exec(ctx, "INSERT INTO items VALUES($1,1,'protobuf fixture',NULL)", i+1); err != nil {
					t.Fatal(err)
				}
				headers, err := json.Marshal(frame.Headers)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(ctx, "SELECT rowrelay_outbox.enqueue('events',$1::uuid,$2,$3,$4::jsonb)", frame.ID, frame.Key, frame.Value, string(headers)); err != nil {
					t.Fatal(err)
				}
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			var queue string
			if err = e.DB.QueryRow(ctx, "SELECT 'rowrelay_outbox.' || epoch::text FROM rowrelay_outbox.stream WHERE name='events'").Scan(&queue); err != nil {
				t.Fatal(err)
			}
			if _, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick($1)", queue); err != nil {
				t.Fatal(err)
			}
			if _, err = e.DB.Exec(ctx, "SELECT pgque.ticker($1)", queue); err != nil {
				t.Fatal(err)
			}
			config := e.Config()
			config.OutboxStream = "events"
			runner, err := relay.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close()
			if n, _, err := runner.Step(ctx); err != nil || n != len(frames) {
				t.Fatalf("relay: count=%d error=%v", n, err)
			}
			consumer, err := e.Consumer(kgo.KeepControlRecords())
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			ends, err := kadm.NewClient(e.Kafka).ListEndOffsets(ctx, e.Name)
			if err != nil || ends[e.Name][0].Err != nil {
				t.Fatal("cannot read final Kafka boundary", err)
			}
			var got []*kgo.Record
			last := int64(-1)
			for last+1 < ends[e.Name][0].Offset {
				fetches := consumer.PollRecords(ctx, 100)
				if len(fetches.Errors()) != 0 {
					t.Fatal(fetches.Errors())
				}
				for _, record := range fetches.Records() {
					last = record.Offset
					if !record.Attrs.IsControl() {
						got = append(got, record)
					}
				}
			}
			if len(got) != len(frames) {
				t.Fatal("missing or duplicate wire records", len(got))
			}
			for i, frame := range frames {
				var expected []kgo.RecordHeader
				for _, header := range frame.Headers {
					expected = append(expected, kgo.RecordHeader{Key: header.Key, Value: header.Value})
				}
				expected = append(expected, kgo.RecordHeader{Key: "id", Value: []byte(frame.ID)})
				if !bytes.Equal(frame.Key, got[i].Key) || !bytes.Equal(frame.Value, got[i].Value) || !reflect.DeepEqual(expected, got[i].Headers) {
					t.Fatal("original wire bytes, header order, duplicate, null or empty header changed", i)
				}
			}
			jvm("consume-unavailable") // A fresh JVM has no cached schema to mask lookup failure.
			jvm("consume")
		})
	}
}
