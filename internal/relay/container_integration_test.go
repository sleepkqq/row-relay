//go:build integration && container

package relay_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
)

func TestPackagedRelayDeliversAndStops(t *testing.T) {
	for _, mode := range []string{"fenced", "managed", "managed-takeover"} {
		t.Run(mode, func(t *testing.T) { testPackagedRelay(t, mode) })
	}
}

func TestPackagedRelayInstallsBundledPgQue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "control")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	image := os.Getenv("ROWRELAY_TEST_IMAGE")
	if image == "" {
		image = "rowrelay:local"
	}
	install := func(url, flag string) error {
		return exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "host", "--read-only",
			"-e", "DATABASE_URL="+url, image, flag).Run()
	}
	if err = install(e.Admin.Config().ConnString(), "--install-pgque"); err != nil {
		t.Fatal("bundled PgQue installation failed", err)
	}
	if _, err = e.Admin.Exec(ctx, "GRANT pgque_admin TO rowrelay_owner"); err != nil {
		t.Fatal(err)
	}
	if err = install(e.URL, "--install"); err != nil {
		t.Fatal("source bootstrap failed", err)
	}
	var epoch string
	if err = e.DB.QueryRow(ctx, "SELECT epoch::text FROM rowrelay.source").Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if install(e.Admin.Config().ConnString(), "--install-pgque") == nil || install(e.URL, "--install") == nil {
		t.Fatal("installer accepted an existing installation")
	}
	var preserved bool
	if err = e.DB.QueryRow(ctx, "SELECT epoch::text=$1 AND EXISTS(SELECT FROM pgque.queue WHERE queue_name='rowrelay') FROM rowrelay.source", epoch).Scan(&preserved); err != nil || !preserved {
		t.Fatal("repeated installation changed the source", err)
	}
}

func testPackagedRelay(t *testing.T, mode string) {
	t.Helper()
	image := os.Getenv("ROWRELAY_TEST_IMAGE")
	if image == "" {
		image = "rowrelay:local"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "outbox-pgque")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	name := "rowrelay-" + e.Name
	docker := func(args ...string) []byte {
		t.Helper()
		data, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v: %s", args[0], err, data)
		}
		return data
	}
	defer func() {
		cleanup, finish := context.WithTimeout(context.Background(), 10*time.Second)
		defer finish()
		if t.Failed() {
			log, _ := exec.CommandContext(cleanup, "docker", "logs", name).CombinedOutput()
			t.Log(string(log))
		}
		_ = exec.CommandContext(cleanup, "docker", "rm", "-f", name).Run()
	}()
	args := []string{"run", "-d", "--name", name, "--network", "host", "--read-only", "--cap-drop=ALL",
		"--security-opt=no-new-privileges", "--memory=128m", "--cpus=1", "--pids-limit=64",
		"-e", "GOMAXPROCS=1", "-e", "GOMEMLIMIT=96MiB", "-e", "DATABASE_URL=" + e.URL,
		"-e", "KAFKA_BROKERS=" + lab.Broker, image, "--delivery-mode", strings.TrimSuffix(mode, "-takeover"),
		"--topic", e.Name, "--outbox-stream", "events", "--poll", "10ms"}
	if mode == "managed-takeover" {
		args = append(args, "--managed-takeover")
	}
	docker(args...)
	if err := lab.WriteOutbox(ctx, e.DB, []int64{1}, []int64{time.Now().UnixNano()}, []string{"container-wire"}); err != nil {
		t.Fatal(err)
	}
	reader, err := e.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for {
		fetches := reader.PollRecords(ctx, 1)
		if errs := fetches.Errors(); len(errs) != 0 {
			t.Fatal(errs)
		}
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		r := records[0]
		if string(r.Key) != "1" || len(r.Value) < 16 || string(r.Value[16:]) != "container-wire" ||
			len(r.Headers) != 1 || r.Headers[0].Key != "id" || string(r.Headers[0].Value) != "00000000-0000-0000-0000-000000000001" {
			t.Fatal("packaged relay changed the prepared record")
		}
		break
	}
	for {
		var done bool
		if err := e.DB.QueryRow(ctx, "SELECT NOT EXISTS (SELECT FROM pgque.subscription WHERE sub_batch IS NOT NULL)").Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if user := strings.TrimSpace(string(docker("inspect", "-f", "{{.Config.User}}", name))); user != "65532:65532" {
		t.Fatalf("unexpected runtime user: %s", user)
	}
	docker("stop", "--time", "5", name)
	var state struct {
		ExitCode  int
		OOMKilled bool
		Running   bool
	}
	if err := json.Unmarshal(docker("inspect", "-f", "{{json .State}}", name), &state); err != nil || state.Running || state.OOMKilled || state.ExitCode != 0 {
		t.Fatal("packaged relay did not stop cleanly", state, err)
	}
}
