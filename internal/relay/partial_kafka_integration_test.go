//go:build integration

package relay_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPartialKafkaAcceptanceDoesNotAcknowledgeSource(t *testing.T) {
	for _, deliveryMode := range []string{"fenced", "managed"} {
		const mode = "pgque"
		t.Run(deliveryMode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			e, err := lab.New(ctx, "outbox-"+mode)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			topic := e.Name + "_partial"
			admin := kadm.NewClient(e.Kafka)
			limit := "512"
			if _, err = admin.CreateTopic(ctx, 2, 1, map[string]*string{"max.message.bytes": &limit}, topic); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_, _ = admin.DeleteTopics(cleanup, topic)
			}()
			if err = outbox.InstallStream(ctx, e.DB, "partial", topic); err != nil {
				t.Fatal(err)
			}
			cfg := e.Config()
			cfg.DeliveryMode = deliveryMode
			cfg.OutboxStream, cfg.Topic, cfg.Compression = "partial", topic, "none"
			r, err := relay.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			for {
				_, advanced, err := r.Step(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !advanced {
					break
				}
			}
			keys := make([][]byte, 2)
			partitioner := kgo.StickyKeyPartitioner(nil).ForTopic(topic)
			for i := 0; keys[0] == nil || keys[1] == nil; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				keys[partitioner.Partition(&kgo.Record{Key: key}, 2)] = key
			}
			values := [][]byte{[]byte("small"), bytes.Repeat([]byte("x"), 4096)}
			for i := range keys {
				_, err = e.DB.Exec(ctx, "SELECT rowrelay_outbox.enqueue('partial',$1::uuid,$2,$3)",
					fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1), keys[i], values[i])
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "pgque" {
				_, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick(queue_name) FROM pgque.queue; SELECT pgque.ticker(queue_name) FROM pgque.queue")
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err = r.Step(ctx); err == nil {
				t.Fatal("partially rejected Kafka batch succeeded")
			}
			if _, _, err = r.Step(ctx); err == nil {
				t.Fatal("failed publisher was reused")
			}
			// Observe the accepted fragment using an intentionally unsafe reader.
			// This proves partial broker success rather than total request rejection.
			consumer := func(committed bool) *kgo.Client {
				level := kgo.ReadUncommitted()
				if committed {
					level = kgo.ReadCommitted()
				}
				c, err := kgo.NewClient(kgo.SeedBrokers(lab.Broker), kgo.FetchMaxWait(10*time.Millisecond),
					kgo.FetchIsolationLevel(level), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
						topic: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart()},
					}))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(c.Close)
				return c
			}
			unsafe := consumer(false)
			fragment := unsafe.PollRecords(ctx, 1).Records()
			if len(fragment) != 1 || !bytes.Equal(fragment[0].Value, values[0]) {
				t.Fatal("partial acceptance not exercised")
			}
			safe := consumer(true)
			poll, stop := context.WithTimeout(ctx, 150*time.Millisecond)
			visible := safe.PollRecords(poll, 10).Records()
			if deliveryMode == "managed" {
				if len(visible) != 1 || !bytes.Equal(visible[0].Value, values[0]) {
					t.Fatal("managed partial success must be visible even to read_committed")
				}
			} else if len(visible) != 0 {
				stop()
				t.Fatal("uncommitted partial batch escaped")
			}
			stop()
			var pending int
			query := "SELECT count(*) FROM pgque.subscription WHERE sub_batch IS NOT NULL"
			want := 1
			if err = e.DB.QueryRow(ctx, query).Scan(&pending); err != nil || pending != want {
				t.Fatal("source acknowledged partial broker success", pending, err)
			}
			r.Close()
			limit = "16384"
			changes, err := admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &limit}}, topic)
			if err != nil {
				t.Fatal(err)
			}
			for _, change := range changes {
				if change.Err != nil {
					t.Fatal(change.Err)
				}
			}
			r, err = relay.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if n, _, err := r.Step(ctx); err != nil || n != 2 {
				t.Fatal("retry failed to replay complete source", n, err)
			}
			seen := map[string]bool{}
			for len(seen) < 2 && ctx.Err() == nil {
				for _, record := range safe.PollRecords(ctx, 2-len(seen)).Records() {
					key := string(record.Key)
					if seen[key] {
						t.Fatal("aborted fragment duplicated after recovery")
					}
					matched := false
					for i := range keys {
						if bytes.Equal(record.Key, keys[i]) && bytes.Equal(record.Value, values[i]) &&
							len(record.Headers) == 1 && record.Headers[0].Key == "id" &&
							string(record.Headers[0].Value) == fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1) {
							matched = true
						}
					}
					if !matched {
						t.Fatal("recovery changed bytes")
					}
					seen[key] = true
				}
			}
			if len(seen) != 2 {
				t.Fatal("recovered transaction incomplete")
			}
		})
	}
}
