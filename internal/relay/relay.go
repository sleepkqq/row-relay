// Package relay delivers source events after Kafka acknowledgement. The default
// fenced profile uses transactions; managed uses a single idempotent publisher.
// Delivery remains at-least-once across databases in both profiles.
package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/cdcwire"
	"github.com/sleepkqq/row-relay/internal/outbox"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrMaintenance = errors.New("source maintenance failed")
var ErrOwnershipUnavailable = errors.New("source is owned by another publisher")

type Config struct {
	DatabaseURL         string
	Brokers             []string
	Topic               string
	SchemaTopics        map[string]string // CDC-only, explicit schema routes in a shared database.
	CDCFormat           string            // Empty/protobuf is the application protocol; legacy-json is for historical fixtures.
	SchemaRegistryURL   string            // Apicurio Confluent-compatible API base URL.
	Compression         string
	DeliveryMode        string // Empty/fenced: transactions. Managed: idempotent, no broker takeover fencing.
	ManagedTakeover     bool   // Outbox only: consumers must atomically deduplicate delivery IDs with their effects.
	ManagedInvalidation bool   // CDC only: consumers invalidate, never materialize rows or perform business effects.
	Poll                time.Duration
	Timeout             time.Duration
	Batch               int
	BatchBytes          int
	OutboxStream        string // Empty selects the CDC source; otherwise a registered wire stream.
	Security            KafkaSecurity
	ProgressInterval    time.Duration // Opt-in CDC control records; zero preserves the legacy data-only topic.
}

func (c Config) Validate() error {
	if c.DatabaseURL == "" || len(c.Brokers) == 0 || (c.Topic == "" && len(c.SchemaTopics) == 0) {
		return errors.New("database, brokers, and topic are required")
	}
	for _, broker := range c.Brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("Kafka broker list contains an empty address")
		}
	}
	if err := c.Security.Validate(); err != nil {
		return err
	}
	if c.Compression != "none" && c.Compression != "zstd" {
		return errors.New("compression must be none or zstd")
	}
	if c.CDCFormat != "" && c.CDCFormat != "protobuf" && c.CDCFormat != "legacy-json" {
		return errors.New("CDC format must be protobuf or legacy-json")
	}
	if c.OutboxStream == "" && c.CDCFormat != "legacy-json" && c.SchemaRegistryURL == "" {
		return errors.New("CDC Protobuf requires a schema registry URL")
	}
	if c.DeliveryMode != "" && c.DeliveryMode != "fenced" && c.DeliveryMode != "managed" {
		return errors.New("delivery mode must be fenced or managed")
	}
	if len(c.SchemaTopics) > 0 {
		if c.Topic != "" || c.OutboxStream != "" || len(c.SchemaTopics) > 16 {
			return errors.New("schema routes require CDC, no default topic, and at most 16 schemas")
		}
		seen := map[string]bool{}
		for schema, topic := range c.SchemaTopics {
			if !regexp.MustCompile(`^[a-z_][a-z0-9_]*$`).MatchString(schema) || topic == "" || seen[topic] {
				return errors.New("invalid schema route or shared CDC topic")
			}
			seen[topic] = true
		}
	}
	if c.ManagedInvalidation && (c.DeliveryMode != "managed" || c.OutboxStream != "" || c.ProgressInterval <= 0) {
		return errors.New("managed invalidation requires CDC and closed-boundary progress")
	}
	if c.DeliveryMode == "managed" && c.ProgressInterval != 0 && !c.ManagedInvalidation {
		return errors.New("managed delivery does not support freshness certificates")
	}
	if c.ManagedTakeover && (c.DeliveryMode != "managed" || c.OutboxStream == "") {
		return errors.New("managed takeover requires a registered managed outbox with an idempotent consumer")
	}
	if c.ProgressInterval < 0 || c.ProgressInterval > time.Minute || (c.ProgressInterval > 0 && c.OutboxStream != "") {
		return errors.New("progress requires CDC and an interval within one minute")
	}
	if c.Poll <= 0 || c.Poll > time.Minute || c.Timeout <= 0 || c.Timeout > 10*time.Minute || c.Batch < 1 || c.Batch > 10000 ||
		c.BatchBytes < 1<<20 || c.BatchBytes > 64<<20 {
		return errors.New("invalid poll, timeout, batch count, or byte bound")
	}
	return nil
}

func (c Config) topics() []string {
	if len(c.SchemaTopics) == 0 {
		return []string{c.Topic}
	}
	topics := make([]string, 0, len(c.SchemaTopics))
	for _, topic := range c.SchemaTopics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

// Encode leaves row numbers untouched: the payload is transported as JSON text.
func Encode(epoch string, id int64, payload []byte) ([]byte, error) {
	if id < 1 {
		return nil, errors.New("invalid source record")
	}
	if _, err := decodeChange(payload); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Headers map[string]string `json:"headers"`
		Payload string            `json:"payload"`
	}{map[string]string{"ID": epoch + ":" + strconv.FormatInt(id, 10), "PARTITION_ID": epoch}, string(payload)})
}

// Change keeps exact JSON numbers and SQL NULL/presence for consumer adapters.
type Change struct {
	Schema string          `json:"schema"`
	Table  string          `json:"table"`
	Op     string          `json:"op"`
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
}

func decodeChange(payload []byte) (Change, error) {
	var row Change
	if len(payload) > 524288 || json.Unmarshal(payload, &row) != nil || row.Schema == "" || row.Table == "" {
		return Change{}, errors.New("invalid source record")
	}
	object := func(v []byte) bool { v = bytes.TrimSpace(v); return len(v) > 0 && v[0] == '{' }
	absent := func(v []byte) bool { return len(v) == 0 || bytes.Equal(bytes.TrimSpace(v), []byte("null")) }
	valid := row.Op == "INSERT" && absent(row.Before) && object(row.After) ||
		row.Op == "UPDATE" && object(row.Before) && object(row.After) ||
		row.Op == "DELETE" && object(row.Before) && absent(row.After)
	if !valid {
		return Change{}, errors.New("invalid source operation or row images")
	}
	return row, nil
}

type Runner struct {
	config       Config
	db           *pgx.Conn
	kafka        *kgo.Client
	epoch        string
	queue        string
	lockID       int64
	failed       bool
	lastProgress time.Time
	schemaIDs    map[string]uint32
}

func Open(ctx context.Context, c Config) (*Runner, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	db, err := pgx.Connect(ctx, c.DatabaseURL)
	if err != nil {
		return nil, errors.New("connect source failed")
	}
	r := &Runner{config: c, db: db, queue: "rowrelay"}
	ok := false
	defer func() {
		if !ok {
			r.Close()
		}
	}()
	var locked bool
	lockQuery := "SELECT 1380931404::bigint,pg_try_advisory_lock(1380931404)"
	var lockArgs []any
	if c.OutboxStream != "" {
		lockQuery = "SELECT id,pg_try_advisory_lock(id) FROM (SELECT hashtextextended($1,1380931404) AS id) s"
		lockArgs = []any{"rowrelay_outbox:" + c.OutboxStream}
	}
	if err = db.QueryRow(ctx, lockQuery, lockArgs...).Scan(&r.lockID, &locked); err != nil {
		return nil, errors.New("source ownership check failed")
	}
	if !locked {
		return nil, ErrOwnershipUnavailable
	}
	// Managed takeover permits late duplicates, not broker fencing. Its consumer
	// must commit the delivery receipt and business effects in one transaction.
	idleLimit := strconv.FormatInt((c.Timeout + c.Poll + 5*time.Second).Milliseconds(), 10)
	if c.DeliveryMode == "managed" && !c.ManagedTakeover && !c.ManagedInvalidation {
		idleLimit = "0"
	}
	if _, err = db.Exec(ctx, "SELECT set_config('idle_session_timeout',$1,false),set_config('idle_in_transaction_session_timeout',$1,false)", idleLimit); err != nil {
		return nil, errors.New("configure source ownership timeout failed")
	}
	var mode string
	if c.OutboxStream == "" {
		err = db.QueryRow(ctx, "SELECT epoch::text, queue_mode FROM rowrelay.source").Scan(&r.epoch, &mode)
	} else {
		var topic string
		err = db.QueryRow(ctx, "SELECT epoch::text,queue_mode,topic FROM rowrelay_outbox.stream WHERE name=$1", c.OutboxStream).Scan(&r.epoch, &mode, &topic)
		if err == nil && topic != c.Topic {
			return nil, errors.New("outbox topic does not match registered route")
		}
		r.queue = "rowrelay_outbox." + r.epoch
	}
	if err != nil || mode != "pgque" {
		return nil, errors.New("source is not a supported PgQue installation")
	}
	if c.OutboxStream == "" && c.CDCFormat != "legacy-json" {
		r.schemaIDs, err = cdcwire.Lookup(ctx, c.SchemaRegistryURL, c.topics())
		if err != nil {
			return nil, err
		}
	}
	r.kafka, err = newProducer(c, "rowrelay-"+r.epoch)
	if err != nil {
		return nil, err
	}
	topics, err := kadm.NewClient(r.kafka).ListTopics(ctx, c.topics()...)
	if err != nil {
		return nil, errors.New("output topic metadata unavailable")
	}
	for _, topic := range c.topics() {
		if len(topics[topic].Partitions) == 0 || topics[topic].Err != nil ||
			(c.OutboxStream == "" && len(topics[topic].Partitions) != 1) {
			return nil, errors.New("output topic must exist; CDC requires exactly one partition")
		}
	}
	// Initialize idempotence under source ownership. Only a transactional ID
	// also fences the previous producer and aborts its unfinished transaction.
	if _, _, err = r.kafka.ProducerID(ctx); err != nil {
		return nil, errors.New("initialize Kafka producer failed")
	}
	ok = true
	return r, nil
}

func newProducer(c Config, transactionID string) (*kgo.Client, error) {
	options, err := c.Security.options()
	if err != nil {
		return nil, err
	}
	codec := kgo.NoCompression()
	if c.Compression == "zstd" {
		codec = kgo.ZstdCompression()
	}
	options = append(options, kgo.SeedBrokers(c.Brokers...), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.ProducerBatchCompression(codec),
		kgo.ProducerBatchMaxBytes(1<<20), kgo.MaxBufferedRecords(c.Batch),
		kgo.MaxBufferedBytes(c.BatchBytes), kgo.RecordDeliveryTimeout(c.Timeout))
	// Idempotent writes remain enabled (franz-go's default) in both profiles.
	if c.DeliveryMode != "managed" {
		options = append(options, kgo.TransactionalID(transactionID), kgo.TransactionTimeout(c.Timeout))
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, errors.New("configure Kafka failed")
	}
	return client, nil
}

func (r *Runner) Close() {
	if r.kafka != nil {
		r.kafka.Close()
	}
	if r.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r.db.Close(ctx)
	}
}

// Step ACKs the entire snapshot batch only after all Kafka chunks are acknowledged
// (and committed in fenced mode). Managed-mode fragments are immediately visible.
// There is no per-event acknowledgement inside a PgQue snapshot batch.
func (r *Runner) Step(ctx context.Context) (count int, advanced bool, err error) {
	if r.failed {
		return 0, false, errors.New("publisher failed; reopen under source ownership")
	}
	ctx, activity, finish := idleContext(ctx, r.config.Timeout)
	defer finish()
	defer func() {
		if err != nil {
			if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
				err = context.DeadlineExceeded
			}
			r.failed = true
		}
	}()
	var batchID *int64
	var boundary time.Time
	progressDue := r.progressDue()
	if err = r.db.QueryRow(ctx, "SELECT pgque.next_batch($1, 'rowrelay')", r.queue).Scan(&batchID); err != nil || batchID == nil {
		return 0, false, err
	}
	activity()
	if progressDue {
		// The ordinary ticker's now() precedes its snapshot. This is a closed
		// snapshot boundary only after EVERY record in this batch is delivered.
		if err = r.db.QueryRow(ctx, `SELECT b.batch_end FROM pgque.get_batch_info($1) b
			JOIN pgque.queue q ON q.queue_name=b.queue_name WHERE NOT q.queue_external_ticker`, *batchID).Scan(&boundary); err != nil {
			return 0, false, err
		}
		activity()
	}
	// Use upstream's complete visibility query, not receive(max_return), which
	// returns only a prefix but whose ack finishes the WHOLE snapshot batch.
	var sql string
	if err = r.db.QueryRow(ctx, "SELECT pgque.batch_event_sql($1)", *batchID).Scan(&sql); err != nil {
		return 0, false, err
	}
	activity()
	// Bound cursor fetches by both count and source bytes. A closed FETCH lets us
	// refresh the actual ownership session between slow Kafka chunks.
	var maxBytes int64
	if err = r.db.QueryRow(ctx, "SELECT coalesce(max(octet_length(ev_data)),0) FROM ("+sql+") AS events", pgx.QueryExecModeExec).Scan(&maxBytes); err != nil {
		return 0, false, err
	}
	activity()
	// ponytail: a single large row reduces every fetch; size-aware paging if measured throughput requires it.
	fetchCount := min(int64(r.config.Batch), max(int64(1), int64(r.config.BatchBytes)/max(int64(1), maxBytes)))
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, false, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, "DECLARE rowrelay_batch NO SCROLL CURSOR FOR SELECT ev_id,ev_data FROM ("+sql+") AS events ORDER BY ev_id", pgx.QueryExecModeExec); err != nil {
		return 0, false, err
	}
	activity()
	records := make([]*kgo.Record, 0, r.config.Batch)
	buffered := 0
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}
		activity()
		if err := r.publish(ctx, records); err != nil {
			return err
		}
		activity()
		clear(records)
		records, buffered = records[:0], 0
		return nil
	}
	type sourceEvent struct {
		id  int64
		raw []byte
	}
	for {
		rows, readErr := tx.Query(ctx, "FETCH FORWARD "+strconv.FormatInt(fetchCount, 10)+" FROM rowrelay_batch", pgx.QueryExecModeExec)
		if readErr != nil {
			return 0, false, readErr
		}
		events, readErr := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceEvent, error) {
			var event sourceEvent
			err := row.Scan(&event.id, &event.raw)
			return event, err
		})
		if readErr != nil {
			return 0, false, readErr
		}
		activity()
		if len(events) == 0 {
			break
		}
		for i, event := range events {
			id, raw := event.id, event.raw
			events[i].raw = nil
			if err = ctx.Err(); err != nil {
				return 0, false, err
			}
			activity()
			var record *kgo.Record
			if r.config.OutboxStream == "" {
				change, decodeErr := decodeChange(raw)
				if decodeErr != nil {
					return 0, false, decodeErr
				}
				topic := r.config.Topic
				if len(r.config.SchemaTopics) > 0 {
					topic = r.config.SchemaTopics[change.Schema]
					if topic == "" {
						return 0, false, errors.New("captured schema has no configured route")
					}
				}
				var value []byte
				if r.config.CDCFormat == "legacy-json" {
					value, err = Encode(r.epoch, id, raw)
				} else {
					value, err = r.encodeCDC(topic, id, change)
				}
				record = &kgo.Record{Topic: topic, Key: []byte(r.epoch), Value: value}
			} else {
				record, err = outbox.Decode(raw)
				if err == nil {
					record.Topic = r.config.Topic
				}
			}
			if err != nil {
				return 0, false, err
			}
			size := len(record.Key) + len(record.Value)
			for _, header := range record.Headers {
				size += len(header.Key) + len(header.Value) + 10
			}
			if size > (1<<20)-1024 {
				return 0, false, errors.New("encoded record exceeds Kafka byte limit")
			}
			if len(records) == r.config.Batch || buffered+size > r.config.BatchBytes {
				if err = flush(); err != nil {
					return 0, false, err
				}
			}
			records = append(records, record)
			buffered += size
			count++
		}
	}
	if err = flush(); err != nil {
		return 0, false, err
	}
	if err = tx.Rollback(ctx); err != nil {
		return 0, false, err
	}
	activity()
	err = r.acknowledge(ctx, *batchID)
	if err == nil && progressDue {
		activity()
		err = r.publishProgress(ctx, boundary)
	}
	return count, true, err
}

// Run uses a separate connection for ticks/maintenance so large snapshot batches
// cannot starve their own ticker. It stops on any source/maintenance failure.
func (r *Runner) Run(ctx context.Context) error {
	return r.RunObserved(ctx, nil)
}

// RunObserved reports completed source steps, including idle reads. This is an
// operability signal only; it must not be used as a CDC freshness checkpoint.
func (r *Runner) RunObserved(ctx context.Context, completed func()) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	maintenance := make(chan error, 1)
	go func() {
		err := r.maintain(ctx)
		maintenance <- err
		cancel()
	}()
	var runErr error
	for ctx.Err() == nil {
		_, advanced, err := r.Step(ctx)
		if err != nil {
			runErr = err
			break
		}
		if completed != nil {
			completed()
		}
		if !advanced {
			select {
			case <-ctx.Done():
			case <-time.After(r.config.Poll):
			}
		}
	}
	cancel()
	maintErr := <-maintenance
	if maintErr != nil && !errors.Is(maintErr, context.Canceled) {
		return errors.Join(ErrMaintenance, maintErr)
	}
	return runErr
}

func (r *Runner) maintain(ctx context.Context) error {
	db, err := pgx.Connect(ctx, r.config.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		db.Close(closeCtx)
	}()
	ticker := time.NewTicker(r.config.Poll)
	defer ticker.Stop()
	lastMaintenance := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			callCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
			_, err = db.Exec(callCtx, "SELECT pgque.ticker($1)", r.queue)
			if err == nil && time.Since(lastMaintenance) >= 10*time.Second {
				_, err = db.Exec(callCtx, "SELECT pgque.maint()")
				if err == nil {
					// Separate transaction: rotation steps must not share a snapshot.
					_, err = db.Exec(callCtx, "SELECT pgque.maint_rotate_tables_step2()")
				}
				lastMaintenance = time.Now()
			}
			cancel()
			if err != nil {
				return err
			}
		}
	}
}
