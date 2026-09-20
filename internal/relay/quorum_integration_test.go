//go:build integration && quorum

package relay_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaQuorumLossPreservesSourceAndRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	brokers := []string{lab.Broker, "127.0.0.1:29093", "127.0.0.1:29094"}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	wait := func(check func(context.Context) bool) {
		t.Helper()
		deadline, stop := context.WithTimeout(ctx, 45*time.Second)
		defer stop()
		for deadline.Err() == nil {
			attempt, finish := context.WithTimeout(deadline, 2*time.Second)
			ok := check(attempt)
			finish()
			if ok {
				return
			}
			select {
			case <-deadline.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatal("quorum fixture did not reach the required broker/ISR state")
	}
	wait(func(ctx context.Context) bool {
		all, err := admin.ListBrokers(ctx)
		return err == nil && len(all) == 3
	})
	e, err := lab.NewWithReplication(ctx, "outbox-pgque", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	containers := map[int32]string{1: "rowrelay-lab-kafka-1", 2: "rowrelay-lab-kafka2-1", 3: "rowrelay-lab-kafka3-1"}
	stopped := map[int32]bool{}
	docker := func(action string, id int32) {
		t.Helper()
		name, ok := containers[id]
		if !ok {
			t.Fatal("unexpected fixture broker ID", id)
		}
		commandCtx, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		if output, err := exec.CommandContext(commandCtx, "docker", action, name).CombinedOutput(); err != nil {
			t.Fatalf("fixture broker %s: %s: %v", action, output, err)
		}
		stopped[id] = action == "kill"
	}
	defer func() {
		// Restore only nodes killed by this test, even on an assertion failure.
		recovery, stop := context.WithTimeout(context.Background(), 45*time.Second)
		defer stop()
		for id, down := range stopped {
			if down {
				if output, err := exec.CommandContext(recovery, "docker", "start", containers[id]).CombinedOutput(); err != nil {
					t.Errorf("restore fixture broker: %s: %v", output, err)
				}
			}
		}
	}()
	partition := func(ctx context.Context) (kadm.PartitionDetail, bool) {
		topics, err := admin.ListTopics(ctx, e.Name)
		topic, ok := topics[e.Name]
		p, exists := topic.Partitions[0]
		return p, err == nil && ok && exists && topic.Err == nil && p.Err == nil && len(p.Replicas) == 3
	}
	var leader int32
	wait(func(ctx context.Context) bool {
		p, ok := partition(ctx)
		leader = p.Leader
		return ok && len(p.ISR) == 3 && leader > 0
	})
	reader, err := e.Consumer(kgo.KeepControlRecords())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	config := e.Config()
	config.OutboxStream, config.Brokers = "events", brokers
	var queue string
	if err := e.DB.QueryRow(ctx, "SELECT 'rowrelay_outbox.' || epoch::text FROM rowrelay_outbox.stream WHERE name='events'").Scan(&queue); err != nil {
		t.Fatal(err)
	}
	enqueue := func(id int64) {
		t.Helper()
		if err := lab.WriteOutbox(ctx, e.DB, []int64{id}, []int64{42}, []string{fmt.Sprintf("quorum-%d", id)}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.DB.Exec(ctx, "SELECT pgque.force_next_tick($1)", queue); err != nil {
			t.Fatal(err)
		}
		if _, err := e.DB.Exec(ctx, "SELECT pgque.ticker($1)", queue); err != nil {
			t.Fatal(err)
		}
	}
	open := func() *relay.Runner {
		t.Helper()
		r, err := relay.Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	seen, lastOffset := map[int64]bool{}, int64(-1)
	receive := func(target int, end int64) {
		t.Helper()
		for len(seen) < target || lastOffset+1 < end {
			fetches := reader.PollRecords(ctx, 100)
			if errs := fetches.Errors(); len(errs) != 0 {
				t.Fatal(errs)
			}
			for _, record := range fetches.Records() {
				lastOffset = record.Offset
				if record.Attrs.IsControl() {
					continue
				}
				if len(record.Value) < 16 {
					t.Fatal("truncated record")
				}
				id := int64(binary.BigEndian.Uint64(record.Value))
				if id < 1 || id > 3 || binary.BigEndian.Uint64(record.Value[8:]) != 42 ||
					string(record.Value[16:]) != fmt.Sprintf("quorum-%d", id) || string(record.Key) != strconv.FormatInt(id, 10) ||
					len(record.Headers) != 1 || record.Headers[0].Key != "id" || string(record.Headers[0].Value) != fmt.Sprintf("00000000-0000-0000-0000-%012x", id) {
					t.Fatal("changed or phantom event after quorum recovery")
				}
				seen[id] = true // Ambiguous commits may replay the same immutable ID.
			}
		}
	}
	enqueue(1)
	r := open()
	defer func() { r.Close() }()
	if n, _, err := r.Step(ctx); err != nil || n != 1 {
		t.Fatal("initial replicated delivery", n, err)
	}
	receive(1, 0)
	docker("kill", leader)
	var survivor int32
	wait(func(ctx context.Context) bool {
		p, ok := partition(ctx)
		survivor = p.Leader
		return ok && len(p.ISR) == 2 && survivor > 0 && survivor != leader
	})
	enqueue(2)
	if n, _, err := r.Step(ctx); err != nil || n != 1 {
		t.Fatal("delivery after one broker/leader failure", n, err)
	}
	receive(2, 0)
	var second int32
	for id := range containers {
		if id != leader && id != survivor {
			second = id
		}
	}
	docker("kill", second) // One node cannot satisfy data min-ISR or controller quorum.
	enqueue(3)
	failure, stop := context.WithTimeout(ctx, 3*time.Second)
	_, _, err = r.Step(failure)
	stop()
	if err == nil {
		t.Fatal("source advanced without Kafka quorum")
	}
	var pending bool
	if err := e.DB.QueryRow(ctx, "SELECT sub_batch IS NOT NULL FROM pgque.subscription").Scan(&pending); err != nil || !pending {
		t.Fatal("unconfirmed batch was acknowledged", err)
	}
	r.Close()
	docker("start", second)
	wait(func(ctx context.Context) bool {
		p, ok := partition(ctx)
		return ok && len(p.ISR) >= 2 && p.Leader > 0 && p.Leader != leader
	})
	r = open()
	if n, _, err := r.Step(ctx); err != nil || n != 1 {
		t.Fatal("recovery delivery", n, err)
	}
	if n, _, err := r.Step(ctx); err != nil || n != 0 {
		t.Fatal("source ACK did not settle", n, err)
	}
	ends, err := admin.ListEndOffsets(ctx, e.Name)
	if err != nil || ends[e.Name][0].Err != nil {
		t.Fatal("final Kafka watermark unavailable", err)
	}
	receive(3, ends[e.Name][0].Offset)
	docker("start", leader)
	wait(func(ctx context.Context) bool {
		p, ok := partition(ctx)
		return ok && len(p.ISR) == 3
	})
}
