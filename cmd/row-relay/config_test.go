// Copyright 2026 sleepkqq
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// runCLI exercises the CLI in-process so no dependency is contacted. lookup
// mirrors os.LookupEnv for the supplied environment.
func runCLI(t *testing.T, args []string, env map[string]string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	getenv := func(key string) string { return env[key] }
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	code := run(args, getenv, lookup, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func writeConfig(t *testing.T, streams []map[string]any) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"streams": streams})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "streams.json")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func multiStreamConfig(t *testing.T, env map[string]string, count, batchBytes int) string {
	t.Helper()
	streams := make([]map[string]any, 0, count)
	for i := 0; i < count; i++ {
		key := "STREAM_DB_" + strconv.Itoa(i)
		env[key] = "postgres://relay:pw@127.0.0.1:5432/db" + strconv.Itoa(i)
		streams = append(streams, map[string]any{
			"name":         "s" + strconv.Itoa(i),
			"database_env": key,
			"topic":        "cdc",
			"cdc_format":   "legacy-json",
			"batch_bytes":  batchBytes,
		})
	}
	return writeConfig(t, streams)
}

func TestParseErrorsDoNotEchoAdversarialArguments(t *testing.T) {
	for _, args := range [][]string{
		{"--check-config", "--poll=topsecretvalue"},
		{"--check-config", "--batch=topsecretvalue"},
		{"--check-config", "--bogus=topsecretvalue"},
	} {
		code, stdout, stderr := runCLI(t, args, map[string]string{"KAFKA_BROKERS": "127.0.0.1:9092"})
		if code != 2 {
			t.Fatalf("args=%v exit=%d stdout=%q stderr=%q", args, code, stdout, stderr)
		}
		if combined := stdout + stderr; strings.Contains(combined, "topsecretvalue") {
			t.Fatalf("args=%v leaked adversarial value: %q", args, combined)
		}
		if !strings.Contains(stderr, "invalid command-line arguments") {
			t.Fatalf("args=%v missing fixed parse error: %q", args, stderr)
		}
	}
}

func TestHelpStaysFunctional(t *testing.T) {
	code, stdout, stderr := runCLI(t, []string{"--help"}, map[string]string{})
	if code != 0 || stderr != "" {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "Usage of row-relay") || !strings.Contains(stdout, "check-config") {
		t.Fatalf("usage not printed: %q", stdout)
	}
}

func TestCheckConfigRejectsFlagCombinations(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"positional", []string{"--check-config", "extra"}},
		{"version", []string{"--check-config", "--version"}},
		{"install", []string{"--check-config", "--install"}},
		{"install-pgque", []string{"--check-config", "--install-pgque"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runCLI(t, tc.args, map[string]string{})
			if code != 2 {
				t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
		})
	}
}

func TestVersionOutputExact(t *testing.T) {
	code, stdout, stderr := runCLI(t, []string{"--version"}, map[string]string{})
	if code != 0 || stdout != "row-relay "+version+"\n" || stderr != "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestCheckConfigSingleSourceSafeSummary(t *testing.T) {
	env := map[string]string{
		"DATABASE_URL":  "postgres://relay:supersecret@db.internal:5432/app",
		"KAFKA_BROKERS": "broker.internal:9092",
	}
	code, stdout, stderr := runCLI(t, []string{"--check-config", "--topic", "cdc", "--cdc-format", "legacy-json"}, env)
	if code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "row-relay check-config version=") || !strings.Contains(stdout, "count=1") {
		t.Fatalf("unexpected summary %q", stdout)
	}
	combined := stdout + stderr
	for _, secret := range []string{"supersecret", "db.internal", "broker.internal", "relay:supersecret"} {
		if strings.Contains(combined, secret) {
			t.Fatalf("summary leaked %q: %q", secret, combined)
		}
	}
}

func TestCheckConfigStreamBudget(t *testing.T) {
	t.Run("overflow-64MiB-rejected", func(t *testing.T) {
		env := map[string]string{"KAFKA_BROKERS": "127.0.0.1:9092"}
		path := multiStreamConfig(t, env, 17, 4<<20)
		code, _, stderr := runCLI(t, []string{"--check-config", "--config", path}, env)
		if code != 2 || !strings.Contains(stderr, "64 MiB") {
			t.Fatalf("exit=%d stderr=%q", code, stderr)
		}
	})
	t.Run("within-64MiB-accepted", func(t *testing.T) {
		env := map[string]string{"KAFKA_BROKERS": "127.0.0.1:9092"}
		path := multiStreamConfig(t, env, 20, 2<<20)
		code, stdout, stderr := runCLI(t, []string{"--check-config", "--config", path}, env)
		if code != 0 {
			t.Fatalf("exit=%d stderr=%q", code, stderr)
		}
		if !strings.Contains(stdout, "count=20") || !strings.Contains(stdout, "total_batch_bytes=41943040") {
			t.Fatalf("unexpected summary %q", stdout)
		}
	})
}

func TestCheckConfigRejectsTooManyStreams(t *testing.T) {
	env := map[string]string{"KAFKA_BROKERS": "127.0.0.1:9092"}
	path := multiStreamConfig(t, env, 33, 1<<20)
	code, _, stderr := runCLI(t, []string{"--check-config", "--config", path}, env)
	if code != 2 || !strings.Contains(stderr, "1..32 streams") {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
}

func TestCheckConfigRejectsRoutingCombinationWithoutSideEffects(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"--check-config", "--config", "missing.json", "--topic", "t"}, map[string]string{})
	if code != 1 || !strings.Contains(stderr, "config cannot be combined") {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
}

func TestCheckConfigFileErrorHidesPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sensitive-config-name.json")
	code, _, stderr := runCLI(t, []string{"--check-config", "--config", path}, map[string]string{"KAFKA_BROKERS": "127.0.0.1:9092"})
	if code != 2 || !strings.Contains(stderr, "cannot read stream configuration") {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stderr, "sensitive-config-name") {
		t.Fatalf("file error leaked path: %q", stderr)
	}
}

func TestCheckConfigSafeValidationReason(t *testing.T) {
	env := map[string]string{
		"KAFKA_BROKERS": "broker.internal:9092",
		"STREAM_DB_0":   "postgres://leakuser:leakpass@leakhost:5432/leakdb",
	}
	// Missing topic fails Config.Validate; only the safe constant is printable.
	path := writeConfig(t, []map[string]any{{
		"name": "s0", "database_env": "STREAM_DB_0", "cdc_format": "legacy-json",
	}})
	code, stdout, stderr := runCLI(t, []string{"--check-config", "--config", path}, env)
	if code != 2 || !strings.Contains(stderr, "invalid stream configuration") {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	combined := stdout + stderr
	for _, secret := range []string{"leakuser", "leakpass", "leakhost", "leakdb", "broker.internal"} {
		if strings.Contains(combined, secret) {
			t.Fatalf("validation output leaked %q: %q", secret, combined)
		}
	}
}
