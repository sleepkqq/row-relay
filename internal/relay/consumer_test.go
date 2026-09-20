package relay

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func trackerFixture(t *testing.T) (*ProgressTracker, time.Time) {
	t.Helper()
	g, err := NewProgressTracker("cdc", "epoch", [][2]string{{"public", "items"}}, 5*time.Second, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	g.started = now
	return g, now
}

func TestCacheFillCannotOvertakeInvalidationOrChangeGeneration(t *testing.T) {
	g, _ := trackerFixture(t)
	now := time.Now()
	g.started = now.Add(-time.Second)
	if err := g.Apply(certificate(t, 0, now), nil); err != nil {
		t.Fatal(err)
	}
	read, ok := g.BeginCacheRead()
	if !ok {
		t.Fatal("fresh cache did not issue a read token")
	}
	value, err := Encode("epoch", 1, []byte(`{"schema":"public","table":"items","op":"UPDATE","before":{"id":1,"fk":7},"after":{"id":1,"fk":8}}`))
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "cdc", Key: []byte("epoch"), Offset: 1, Value: value}
	var mu sync.Mutex
	cache := map[string]int{"entity:1": 7}
	invalidate := func(Change) error {
		mu.Lock()
		defer mu.Unlock()
		delete(cache, "entity:1")
		return nil
	}
	if err := g.Apply(record, invalidate); err != nil {
		t.Fatal(err)
	}
	if err := g.Apply(certificate(t, 2, time.Now()), nil); err != nil {
		t.Fatal(err)
	}
	if g.CacheReadValid(read) || g.StoreCache(read, func() { t.Error("stale fill executed") }) {
		t.Fatal("a later certificate resurrected an invalidated read/fill")
	}
	fresh, ok := g.BeginCacheRead()
	if !ok || !g.CacheReadValid(fresh) {
		t.Fatal("new read was rejected")
	}
	other, _ := trackerFixture(t)
	other.started = now.Add(-time.Second)
	if err := other.Apply(certificate(t, 0, time.Now()), nil); err != nil {
		t.Fatal(err)
	}
	if other.StoreCache(fresh, func() { t.Error("cross-generation fill executed") }) {
		t.Fatal("another namespace accepted the token")
	}
	// If storage wins the fence, the next invalidation must run AFTER it, so a
	// late cache write can never undo an already-completed invalidation.
	entered, release := make(chan struct{}), make(chan struct{})
	stored := make(chan bool, 1)
	go func() {
		stored <- g.StoreCache(fresh, func() {
			close(entered)
			<-release
			mu.Lock()
			defer mu.Unlock()
			cache["entity:1"] = 8
		})
	}()
	<-entered
	applied := make(chan error, 1)
	record.Offset = 3
	go func() { applied <- g.Apply(record, invalidate) }()
	close(release)
	if !<-stored {
		t.Fatal("valid fill rejected")
	}
	if err := <-applied; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	remaining := len(cache)
	mu.Unlock()
	if remaining != 0 {
		t.Fatal("fill escaped invalidation")
	}
	fresh, _ = g.BeginCacheRead()
	g.mu.Lock()
	g.expiry = time.Now().Add(-time.Nanosecond)
	g.mu.Unlock()
	if g.CacheReadValid(fresh) || g.StoreCache(fresh, func() { t.Error("expired fill executed") }) {
		t.Fatal("expired readiness left a usable token")
	}
	if err := g.Apply(certificate(t, 4, time.Now()), nil); err != nil {
		t.Fatal(err)
	}
	fresh, _ = g.BeginCacheRead()
	g.Fail()
	if g.CacheReadValid(fresh) || g.StoreCache(fresh, func() { t.Error("failed-generation fill executed") }) {
		t.Fatal("terminal failure left a usable token")
	}
}

func certificate(t *testing.T, offset int64, at time.Time) *kgo.Record {
	t.Helper()
	value, err := json.Marshal(map[string]any{"rowrelay_progress": Progress{1, "epoch", at}})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "cdc", Key: []byte("epoch"), Offset: offset, Value: value}
}

func TestColdProgressWaitsForApplyAndExpiresWithoutRenewing(t *testing.T) {
	g, start := trackerFixture(t)
	other, _ := trackerFixture(t)
	if g.Namespace() == other.Namespace() || g.readyAt(start) {
		t.Fatal("cache generation was reused or started ready")
	}
	if err := g.applyAt(certificate(t, 0, start.Add(-time.Second)), start, nil); err != nil || g.readyAt(start) {
		t.Fatal("old replay certificate activated cold cache", err)
	}
	raw := []byte(`{"schema":"public","table":"items","op":"UPDATE","before":{"id":9007199254740993,"fk":7},"after":{"id":9007199254740993,"fk":null}}`)
	value, err := Encode("epoch", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "cdc", Key: []byte("epoch"), Offset: 2, Value: value}
	cache := map[string]bool{"entity:9007199254740993": true, "association:7": true}
	apply := func(change Change) error {
		var old, next struct{ ID, FK json.Number }
		if err := json.Unmarshal(change.Before, &old); err != nil {
			return err
		}
		if err := json.Unmarshal(change.After, &next); err != nil {
			return err
		}
		if old.ID.String() != "9007199254740993" || next.FK.String() != "" {
			return errors.New("precision or SQL NULL changed")
		}
		delete(cache, "entity:"+old.ID.String())
		delete(cache, "association:"+old.FK.String())
		return nil
	}
	if err = g.applyAt(record, start.Add(time.Second), apply); err != nil || len(cache) != 0 {
		t.Fatal("old entity/association invalidation failed", err)
	}
	boundary := start.Add(2 * time.Second)
	if err = g.applyAt(certificate(t, 4, boundary), boundary, nil); err != nil || !g.readyAt(boundary) {
		t.Fatal("post-apply closed boundary did not activate cache", err)
	}
	// A source replay has a new Kafka offset but the same stable event identity.
	record.Offset = 6
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- g.applyAt(record, boundary, func(change Change) error {
			close(entered)
			<-release
			return apply(change)
		})
	}()
	<-entered
	readyDuringApply := g.readyAt(boundary)
	close(release)
	if err = <-done; err != nil || readyDuringApply || len(cache) != 0 {
		t.Fatal("replay was non-idempotent or readiness overtook apply", err)
	}
	if err = g.applyAt(certificate(t, 8, boundary), start.Add(4*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if g.readyAt(start.Add(6900 * time.Millisecond)) {
		t.Fatal("repeated certificate or absent heartbeat extended freshness")
	}
}

func TestConsumerFailuresAreTerminalAndDoNotAdvanceApply(t *testing.T) {
	for _, failure := range []string{"apply", "schema", "future", "epoch", "malformed", "version", "offset", "gap"} {
		t.Run(failure, func(t *testing.T) {
			g, start := trackerFixture(t)
			now := start.Add(time.Second)
			if err := g.applyAt(certificate(t, 2, now), now, nil); err != nil {
				t.Fatal(err)
			}
			record := certificate(t, 4, now)
			var apply func(Change) error
			switch failure {
			case "apply", "schema":
				raw := `{"schema":"public","table":"items","op":"INSERT","after":{"id":1}}`
				if failure == "schema" {
					raw = strings.Replace(raw, "items", "unknown", 1)
				}
				record.Value, _ = Encode("epoch", 1, []byte(raw))
				apply = func(Change) error { return errors.New("private failure detail") }
			case "future":
				record = certificate(t, 4, now.Add(time.Second))
			case "epoch":
				record.Key = []byte("other-epoch")
			case "malformed":
				record.Value = []byte(`{"secret":"private"}`)
			case "version":
				record.Value = []byte(strings.Replace(string(record.Value), `"version":1`, `"version":2`, 1))
			case "offset":
				record.Offset = 1
			case "gap":
				g.Fail() // Follow calls Fail for broker offset/retention errors.
			}
			err := g.applyAt(record, now, apply)
			if err == nil || strings.Contains(err.Error(), "private") || g.offset != 2 || g.readyAt(now) {
				t.Fatal("failed apply advanced offset, leaked input, or stayed ready", err)
			}
			if err = g.applyAt(certificate(t, 6, now), now, nil); err == nil || g.readyAt(now) {
				t.Fatal("later checkpoint bypassed terminal failure")
			}
		})
	}
}
