// Package health reports worker operability, never certified CDC freshness.
package health

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type worker struct {
	phase    string
	last     time.Time
	deadline time.Duration
}

type Health struct {
	mu       sync.RWMutex
	workers  map[string]worker
	stopping bool
}

func New(deadlines map[string]time.Duration) *Health {
	h := &Health{workers: make(map[string]worker, len(deadlines))}
	for name, deadline := range deadlines {
		h.workers[name] = worker{phase: "starting", deadline: deadline}
	}
	return h
}

// Success means one source step completed (including an idle read), not that a
// visibility frontier, consumer position, or Kafka availability was certified.
func (h *Health) Success(name string) {
	h.mark(name, "active")
}

// Standby records a successful ownership check: another session holds the
// source. It is an operationally healthy contender, not a freshness proof.
func (h *Health) Standby(name string) {
	h.mark(name, "standby")
}

func (h *Health) mark(name, phase string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if w, ok := h.workers[name]; ok && !h.stopping {
		w.phase, w.last = phase, time.Now()
		h.workers[name] = w
	}
}

func (h *Health) Fail(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if w, ok := h.workers[name]; ok {
		w.phase = "failed"
		h.workers[name] = w
	}
}

func (h *Health) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
}

type snapshot struct {
	Ready              bool              `json:"ready"`
	FreshnessCertified bool              `json:"freshness_certified"`
	Stopping           bool              `json:"stopping"`
	Streams            map[string]string `json:"streams"`
}

func (h *Health) snapshot(now time.Time) snapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s := snapshot{Ready: !h.stopping && len(h.workers) > 0, Stopping: h.stopping, Streams: make(map[string]string, len(h.workers))}
	for name, w := range h.workers {
		phase := w.phase
		if (phase == "active" || phase == "standby") && (w.deadline <= 0 || now.Sub(w.last) > w.deadline) {
			phase = "stalled"
		}
		if phase != "active" && phase != "standby" {
			s.Ready = false
		}
		s.Streams[name] = phase
	}
	return s
}

func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"live\":true}\n"))
	})
	status := func(w http.ResponseWriter, r *http.Request) {
		s := h.snapshot(time.Now())
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/ready" && !s.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(s)
	}
	mux.HandleFunc("GET /ready", status)
	mux.HandleFunc("GET /status", status)
	return mux
}
