//go:build integration

package relay_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
)

func TestTwoProcessesFencePausedOwner(t *testing.T) {
	for _, failure := range []string{"terminate", "idle-expiry"} {
		for _, mode := range []string{"pgque"} {
			for _, wire := range []bool{false, true} {
				name := mode
				if wire {
					name = "outbox-" + mode
				}
				t.Run(failure+"/"+name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
					defer cancel()
					e, err := lab.New(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					defer e.Close()
					cfg := e.Config()
					if failure == "idle-expiry" {
						cfg.Timeout = 2 * time.Second
					}
					if wire {
						cfg.OutboxStream = "events"
						err = lab.WriteOutbox(ctx, e.DB, []int64{1, 2}, []int64{1, 2}, []string{"first", "second"})
					} else {
						err = lab.Write(ctx, e.DB, []int64{1, 2}, []int64{1, 2}, []string{"first", "second"})
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
					command := func(role string) *exec.Cmd {
						cmd := exec.CommandContext(ctx, binary, "-test.run=^TestOwnerProcess$", "-test.timeout=30s")
						cmd.Env = append(os.Environ(), "ROWRELAY_TEST_OWNER_ROLE="+role, "ROWRELAY_TEST_OWNER_CONFIG="+string(config))
						return cmd
					}
					old := command("old")
					output, err := old.StdoutPipe()
					if err != nil {
						t.Fatal(err)
					}
					resume, err := old.StdinPipe()
					if err != nil {
						t.Fatal(err)
					}
					if err = old.Start(); err != nil {
						t.Fatal(err)
					}
					defer func() { resume.Close(); old.Process.Kill(); old.Wait() }()
					reader := bufio.NewReader(output)
					line, err := reader.ReadString('\n')
					if err != nil {
						t.Fatal("old owner did not reach pause", err, line)
					}
					var pid uint32
					if _, err = fmt.Sscanf(line, "READY %d\n", &pid); err != nil {
						t.Fatal("old owner startup", line)
					}
					if failure == "terminate" {
						var terminated bool
						if err = e.Admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
							t.Fatal("terminate owner session", err)
						}
					} else {
						for {
							var alive bool
							if err = e.Admin.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", pid).Scan(&alive); err != nil {
								t.Fatal(err)
							}
							if !alive {
								break
							}
							select {
							case <-ctx.Done():
								t.Fatal("idle owner never expired")
							case <-time.After(50 * time.Millisecond):
							}
						}
					}
					if output, err := command("new").CombinedOutput(); err != nil || !strings.Contains(string(output), "DELIVERED") {
						t.Fatal("successor", err, string(output))
					}
					if _, err = resume.Write([]byte{1}); err != nil {
						t.Fatal(err)
					}
					line, err = reader.ReadString('\n')
					if err != nil || strings.TrimSpace(line) != "FENCED" {
						t.Fatal("old process was not fenced", err, line)
					}
					if err = old.Wait(); err != nil {
						t.Fatal("old process failed fencing assertions", err)
					}
					consumer, err := e.Consumer()
					if err != nil {
						t.Fatal(err)
					}
					defer consumer.Close()
					var values [][]byte
					for len(values) < 2 && ctx.Err() == nil {
						for _, record := range consumer.PollRecords(ctx, 2-len(values)).Records() {
							values = append(values, record.Value)
						}
					}
					if len(values) != 2 || !strings.Contains(string(values[0]), "first") || !strings.Contains(string(values[1]), "second") {
						t.Fatal("committed event order changed")
					}
					poll, stop := context.WithTimeout(ctx, 100*time.Millisecond)
					defer stop()
					if consumer.PollRecords(poll, 1).NumRecords() != 0 {
						t.Fatal("stale transaction became visible to read_committed")
					}
				})
			}
		}
	}
}
