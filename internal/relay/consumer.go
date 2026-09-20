package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProgressTracker is a cold-cache reference consumer. Callers use Namespace for
// ALL cache keys and fence reads/fills with BeginCacheRead/CacheReadValid/StoreCache.
// Apply must perform idempotent invalidation, including old associations, before
// returning. This does not supply business-effect exactly-once semantics.
type ProgressTracker struct {
	mu                          sync.RWMutex
	topic, epoch, namespace     string
	tables                      map[[2]string]bool
	started, boundary, expiry   time.Time
	maxLag, skew                time.Duration
	offset                      int64
	revision                    uint64
	failed, applying, following bool
}

func NewProgressTracker(topic, epoch string, tables [][2]string, maxLag, skew time.Duration) (*ProgressTracker, error) {
	if topic == "" || epoch == "" || len(tables) == 0 || maxLag <= 0 || skew < 0 || skew >= maxLag {
		return nil, errors.New("invalid consumer scope or clock bounds")
	}
	t := &ProgressTracker{topic: topic, epoch: epoch, namespace: "rowrelay-cache-" + rand.Text(),
		tables: make(map[[2]string]bool), started: time.Now(), maxLag: maxLag, skew: skew, offset: -1}
	for _, table := range tables {
		if table[0] == "" || table[1] == "" {
			return nil, errors.New("empty consumer table")
		}
		t.tables[table] = true
	}
	return t, nil
}

func (t *ProgressTracker) Namespace() string { return t.namespace }

func (t *ProgressTracker) Ready() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.readyLocked(time.Now())
}

func (t *ProgressTracker) readyAt(now time.Time) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.readyLocked(now)
}

func (t *ProgressTracker) readyLocked(now time.Time) bool {
	return !t.failed && !t.applying && now.Before(t.expiry)
}

// CacheRead is an opaque token for one cache lookup or a subsequent fresh DB
// read. Obtain it BEFORE opening the database read's snapshot/transaction.
type CacheRead struct {
	tracker  *ProgressTracker
	revision uint64
}

func (t *ProgressTracker) BeginCacheRead() (CacheRead, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.readyLocked(time.Now()) {
		return CacheRead{}, false
	}
	return CacheRead{t, t.revision}, true
}

// CacheReadValid must be checked AFTER a cache lookup, before using its result.
func (t *ProgressTracker) CacheReadValid(read CacheRead) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return read.tracker == t && read.revision == t.revision && t.readyLocked(time.Now())
}

// StoreCache atomically fences a completed DB read against invalidation. store
// must be a short, local cache mutation: no I/O, reentry into the tracker, or
// cache lock held by the caller. Invalidations take the cache lock only inside
// Apply's callback, which runs outside the tracker lock.
func (t *ProgressTracker) StoreCache(read CacheRead, store func()) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if store == nil || read.tracker != t || read.revision != t.revision || !t.readyLocked(time.Now()) {
		return false
	}
	store()
	return true
}

func (t *ProgressTracker) Fail() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed, t.expiry = true, time.Time{}
}

// Apply is called serially in Kafka order. Ready may be called concurrently.
// Kafka offsets need not be consecutive: transaction markers and aborted records
// create valid holes. Retention gaps must fail the Kafka reader instead.
func (t *ProgressTracker) Apply(record *kgo.Record, apply func(Change) error) error {
	return t.applyAt(record, time.Now(), apply)
}

func (t *ProgressTracker) applyAt(record *kgo.Record, now time.Time, apply func(Change) error) error {
	t.mu.Lock()
	fail := func() error {
		t.failed, t.expiry = true, time.Time{}
		t.mu.Unlock()
		return errors.New("consumer rejected record; a cold rebuild is required")
	}
	if t.failed || t.applying || record == nil || record.Topic != t.topic || record.Partition != 0 ||
		record.Offset <= t.offset || string(record.Key) != t.epoch || len(record.Value) > 1<<20 {
		return fail()
	}
	var envelope struct {
		Headers  map[string]string `json:"headers"`
		Payload  *string           `json:"payload"`
		Progress *Progress         `json:"rowrelay_progress"`
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF {
		return fail()
	}
	if p := envelope.Progress; p != nil {
		if envelope.Payload != nil || envelope.Headers != nil || p.Version != 1 || p.SourceEpoch != t.epoch ||
			p.CommittedBefore.IsZero() || p.CommittedBefore.Before(t.boundary) || p.CommittedBefore.After(now.Add(t.skew)) {
			return fail()
		}
		expiry := time.Time{}
		if p.CommittedBefore.After(t.started.Add(t.skew)) {
			// Conservatively subtract the declared DB/consumer clock-skew bound.
			// Retain now's monotonic clock so a wall-clock step cannot extend TTL.
			expiry = now.Add(p.CommittedBefore.Add(t.maxLag - t.skew).Sub(now))
			if p.CommittedBefore.Equal(t.boundary) && !t.expiry.IsZero() && expiry.After(t.expiry) {
				expiry = t.expiry // Repeated certificates never renew their own lease.
			}
		}
		t.boundary, t.expiry, t.offset = p.CommittedBefore, expiry, record.Offset
		t.mu.Unlock()
		return nil
	}
	if envelope.Payload == nil || envelope.Headers["PARTITION_ID"] != t.epoch || apply == nil {
		return fail()
	}
	suffix, ok := strings.CutPrefix(envelope.Headers["ID"], t.epoch+":")
	id, err := strconv.ParseInt(suffix, 10, 64)
	if !ok || err != nil || id < 1 || strconv.FormatInt(id, 10) != suffix {
		return fail()
	}
	change, err := decodeChange([]byte(*envelope.Payload))
	if err != nil || !t.tables[[2]string{change.Schema, change.Table}] {
		return fail()
	}
	// ponytail: stream-wide fencing rejects unrelated in-flight fills too; use
	// per-key revisions only if measured rejection cost warrants the complexity.
	t.revision++
	t.applying = true
	t.mu.Unlock()
	err = apply(change)
	t.mu.Lock()
	t.applying = false
	if err != nil || t.failed {
		return fail()
	}
	t.offset = record.Offset
	t.mu.Unlock()
	return nil
}

// Follow runs a reference single-partition, cold-generation consumer. No group
// offset can outrun application: every restart replays retained history into a
// new namespace, and no cache becomes ready before a post-start closed barrier.
func (t *ProgressTracker) Follow(ctx context.Context, brokers []string, security KafkaSecurity, apply func(context.Context, Change) error) error {
	defer t.Fail()
	if len(brokers) == 0 || apply == nil {
		return errors.New("consumer brokers and apply callback are required")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("empty consumer broker")
		}
	}
	t.mu.Lock()
	if t.failed || t.applying || t.following || t.offset != -1 {
		t.mu.Unlock()
		return errors.New("reference consumer requires an unused cold generation")
	}
	t.following = true
	t.mu.Unlock()
	options, err := security.options()
	if err != nil {
		return err
	}
	options = append(options, kgo.SeedBrokers(brokers...), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.MaxConcurrentFetches(1),
		kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(1<<20),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{t.topic: {0: kgo.NoResetOffset().AtStart()}}))
	client, err := kgo.NewClient(options...)
	if err != nil {
		return errors.New("configure reference consumer failed")
	}
	defer client.Close()
	topics, err := kadm.NewClient(client).ListTopics(ctx, t.topic)
	if err != nil || topics[t.topic].Err != nil || len(topics[t.topic].Partitions) != 1 {
		return errors.New("reference consumer requires an existing one-partition topic")
	}
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1000)
		if len(fetches.Errors()) != 0 {
			return errors.New("consumer fetch failed; a cold rebuild is required")
		}
		for _, record := range fetches.Records() {
			if err := t.Apply(record, func(change Change) error {
				call, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				if err := apply(call, change); err != nil {
					return err
				}
				return call.Err()
			}); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
