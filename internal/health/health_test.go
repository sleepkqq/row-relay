package health

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadinessRequiresEveryWorkerAndExpiresWithoutProgress(t *testing.T) {
	h := New(map[string]time.Duration{"cdc": time.Second, "outbox": time.Second})
	check := func(path string, want int) {
		t.Helper()
		r := httptest.NewRecorder()
		h.Handler().ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		if r.Code != want {
			t.Fatalf("%s: %d, want %d", path, r.Code, want)
		}
		if path != "/live" && !strings.Contains(r.Body.String(), `"freshness_certified":false`) {
			t.Fatal("worker health presented as freshness")
		}
	}
	check("/live", 200)
	check("/ready", 503)
	h.Success("cdc")
	check("/ready", 503)
	h.Success("outbox")
	check("/ready", 200)
	h.Standby("outbox")
	check("/ready", 200)
	if s := h.snapshot(time.Now().Add(2 * time.Second)); s.Ready || s.Streams["outbox"] != "stalled" {
		t.Fatal("standby ownership checks did not expire")
	}
	if s := h.snapshot(time.Now().Add(2 * time.Second)); s.Ready || s.Streams["cdc"] != "stalled" {
		t.Fatal("stalled loop remained ready")
	}
	h.Fail("outbox")
	check("/ready", 503)
	check("/status", 200)
	check("/live", 200)
	h.Success("outbox")
	check("/ready", 200)
	h.Stop()
	h.Success("outbox")
	check("/ready", 503)
	check("/live", 200)
}

func TestProbeServerClosesOnCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := New(map[string]time.Duration{"source": time.Second})
	done := make(chan error, 1)
	go func() { done <- h.Serve(ctx, listener) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health listener did not stop")
	}
	if h.snapshot(time.Now()).Ready {
		t.Fatal("stopped server remained ready")
	}
}
