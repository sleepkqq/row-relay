//go:build integration

package relay_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
)

func TestLostKafkaCommitReplyReplaysIdenticalEvents(t *testing.T) {
	for _, role := range []string{"lost-kafka-ack", "unlock-before-ack"} {
		for _, mode := range []string{"pgque"} {
			for _, wire := range []bool{false, true} {
				name := mode
				if wire {
					name = "outbox-" + mode
				}
				t.Run(role+"/"+name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					e, err := lab.New(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					defer e.Close()
					cfg := e.Config()
					if wire {
						cfg.OutboxStream = "events"
						err = lab.WriteOutbox(ctx, e.DB, []int64{1}, []int64{1}, []string{"committed"})
					} else {
						err = lab.Write(ctx, e.DB, []int64{1}, []int64{1}, []string{"committed"})
					}
					if err != nil {
						t.Fatal(err)
					}
					if mode == "pgque" {
						_, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick(queue_name) FROM pgque.queue; SELECT pgque.ticker(queue_name) FROM pgque.queue")
						if err != nil {
							t.Fatal(err)
						}
					}
					config, err := json.Marshal(cfg)
					if err != nil {
						t.Fatal(err)
					}
					binary, err := os.Executable()
					if err != nil {
						t.Fatal(err)
					}
					cmd := exec.CommandContext(ctx, binary, "-test.run=^TestOwnerProcess$", "-test.timeout=25s")
					cmd.Env = append(os.Environ(), "ROWRELAY_TEST_OWNER_ROLE="+role, "ROWRELAY_TEST_OWNER_CONFIG="+string(config))
					if output, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(output), "UNCONFIRMED") {
						t.Fatal("commit response loss", err, string(output))
					}
					consumer, err := e.Consumer()
					if err != nil {
						t.Fatal(err)
					}
					defer consumer.Close()
					first := consumer.PollRecords(ctx, 1).Records()
					if len(first) != 1 {
						t.Fatal("Kafka transaction did not actually commit")
					}
					query, want := "SELECT count(*) FROM pgque.subscription WHERE sub_batch IS NOT NULL", 1
					var pending int
					if err = e.DB.QueryRow(ctx, query).Scan(&pending); err != nil || pending != want {
						t.Fatal("source advanced past unconfirmed Kafka commit", err, pending)
					}
					r, err := relay.Open(ctx, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					if n, _, err := r.Step(ctx); err != nil || n != 1 {
						t.Fatal("replay failed", n, err)
					}
					second := consumer.PollRecords(ctx, 1).Records()
					if len(second) != 1 || !bytes.Equal(first[0].Key, second[0].Key) || !bytes.Equal(first[0].Value, second[0].Value) {
						t.Fatal("ambiguous commit changed event bytes")
					}
					a, _ := json.Marshal(first[0].Headers)
					b, _ := json.Marshal(second[0].Headers)
					if !bytes.Equal(a, b) {
						t.Fatal("replay changed delivery identity headers")
					}
				})
			}
		}
	}
}
