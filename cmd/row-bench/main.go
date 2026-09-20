// row-bench runs one disposable, correctness-reconciled local experiment.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type received struct {
	checksum string
	sent     int64
	latency  float64
}

type sample struct {
	At            float64  `json:"elapsed_s"`
	CPUSeconds    float64  `json:"cpu_seconds"`
	MemoryMiB     float64  `json:"memory_mib"`
	AnonymousMiB  float64  `json:"anonymous_mib"`
	ProcessRSSMiB *float64 `json:"process_rss_mib,omitempty"`
}

func main() {
	mode := flag.String("mode", "pgque", "pgque, pgboss baseline, or capture-disabled control")
	duration := flag.Duration("duration", 60*time.Second, "measured offered-load duration")
	warmup := flag.Duration("warmup", 10*time.Second, "warm-up duration")
	rate := flag.Int("rate", 1000, "offered events/second; 0 measures idle")
	size := flag.Int("payload-bytes", 1024, "synthetic text payload bytes")
	hold := flag.Bool("hold-xmin", false, "hold a repeatable-read snapshot during measured load")
	diagnostics := flag.Bool("diagnostics", false, "sample SQL waits and snapshot batch lag (adds observer work)")
	compression := flag.String("compression", "zstd", "none or zstd")
	poll := flag.Duration("poll", 50*time.Millisecond, "common poll/tick cadence")
	wire := flag.Bool("outbox", false, "measure prepared binary business-outbox records")
	nodeIngress := flag.Bool("node-ingress", false, "common Node SDK ingress and immutable facts for outbox comparisons")
	nodeDirectory := flag.String("node-baseline-dir", "benchmarks/pgboss", "Node fixture directory; runner uses a frozen copy")
	relayBinary := flag.String("relay-binary", "bin/row-relay", "relay executable; runner uses a frozen copy")
	backlog := flag.Int("backlog", 0, "preload this many records before starting relay; requires rate=0 and warmup=0")
	output := flag.String("output", "", "required raw JSON result path")
	flag.Parse()
	if *output == "" || *duration <= 0 || *warmup < 0 || *rate < 0 || *rate > 100000 || *size < 0 || *size > 200000 ||
		(*mode != "pgque" && *mode != "pgboss" && *mode != "control") || *poll <= 0 || *backlog < 0 || *backlog > 1000000 ||
		(*nodeIngress && (!*wire || *mode == "control")) || (*mode == "pgboss" && (!*nodeIngress || *compression != "none")) ||
		(*backlog > 0 && (*rate != 0 || *warmup != 0 || *mode == "control")) {
		fatal(errors.New("invalid benchmark arguments"))
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*duration+3*time.Minute)
	defer cancel()
	fixtureMode := *mode
	if *wire && *mode != "control" {
		fixtureMode = "outbox-" + *mode
	}
	if *mode == "pgboss" {
		fixtureMode = "control" // The official SDK installs and owns its queue schema.
	}
	e, err := lab.New(ctx, fixtureMode)
	if err != nil {
		fatal(err)
	}
	defer func() { cancel(); e.Close() }()
	if err = os.WriteFile(*output+".fixture", []byte(e.Name+"\n"), 0644); err != nil {
		fatal(err)
	}
	queue := "rowrelay"
	if *wire && *mode != "control" && *mode != "pgboss" {
		if err = e.DB.QueryRow(ctx, "SELECT 'rowrelay_outbox.' || epoch::text FROM rowrelay_outbox.stream WHERE name='events'").Scan(&queue); err != nil {
			fatal(err)
		}
	}
	if *mode == "pgque" {
		if _, err = e.DB.Exec(ctx, "SELECT pgque.set_queue_config($1, 'rotation_period', '10 seconds')", queue); err != nil {
			fatal(err)
		}
	}
	write := lab.Write
	if *wire && *mode != "control" {
		write = lab.WriteOutbox
	}
	if *nodeIngress {
		ingress, err := startIngress(ctx, e.URL, *mode, *nodeDirectory, *output)
		if err != nil {
			fatal(err)
		}
		defer ingress.Close()
		write = ingress.Write
	}
	var id int64
	for id < int64(*backlog) {
		n := min(1000, *backlog-int(id))
		ids, sent, bodies := make([]int64, n), make([]int64, n), make([]string, n)
		for i := range ids {
			id++
			ids[i] = id
			sent[i] = time.Now().UnixNano()
			bodies[i] = synthetic(id, *size)
		}
		if err = write(ctx, e.DB, ids, sent, bodies); err != nil {
			fatal(err)
		}
	}
	launchStart := time.Now()
	container := "rowrelay-" + e.Name
	if *mode != "control" {
		binary, _ := filepath.Abs(*relayBinary)
		args := []string{"run", "-d", "--name", container, "--network", "host", "--memory", "128m", "--cpus", "1", "--pids-limit", "64",
			"--security-opt", "no-new-privileges", "-e", "GOMAXPROCS=1", "-e", "GOMEMLIMIT=96MiB",
			"-e", "DATABASE_URL=" + e.URL, "-e", "KAFKA_BROKERS=" + lab.Broker,
			"-v", binary + ":/row-relay:ro", "busybox:1.36", "/row-relay",
			"--topic", e.Name, "--compression", *compression, "--poll", poll.String()}
		if *wire {
			args = append(args, "--outbox-stream", "events")
		}
		if *mode == "pgboss" {
			image, err := os.ReadFile(filepath.Join(*nodeDirectory, "node-image.txt"))
			if err != nil {
				fatal(err)
			}
			root, _ := filepath.Abs(*nodeDirectory)
			args = []string{"run", "-d", "--name", container, "--network", "host", "--memory", "128m", "--cpus", "1", "--pids-limit", "64",
				"--security-opt", "no-new-privileges", "--read-only", "-e", "NODE_OPTIONS=--max-old-space-size=96",
				"-e", "DATABASE_URL=" + e.URL, "-e", "KAFKA_BROKERS=" + lab.Broker, "-e", "KAFKA_TOPIC=" + e.Name,
				"-e", "POLL_MS=" + strconv.FormatInt(poll.Milliseconds(), 10),
				"-e", "BATCH_COUNT=" + strconv.Itoa(min(1000, (4<<20)/(*size+336))),
				"-v", root + ":/baseline:ro", "-w", "/baseline", strings.TrimSpace(string(image)), "node", "worker.mjs"}
		}
		if _, err = command(ctx, "docker", args...); err != nil {
			fatal(err)
		}
		defer func() {
			cancel()
			cleanup, finish := context.WithTimeout(context.Background(), 15*time.Second)
			defer finish()
			log, _ := command(cleanup, "docker", "logs", container)
			_ = os.WriteFile(*output+".container.log", []byte(log), 0644)
			state, _ := command(cleanup, "docker", "inspect", "-f", "{{json .State}}", container)
			_ = os.WriteFile(*output+".state.json", []byte(state), 0644)
			_, _ = command(cleanup, "docker", "rm", "-f", container)
		}()
	}
	consumer, err := e.Consumer(kgo.KeepControlRecords())
	if err != nil {
		fatal(err)
	}
	defer consumer.Close()
	var mu sync.Mutex
	seen := make(map[int64]received)
	duplicates := 0
	var count atomic.Int64
	var lastOffset atomic.Int64
	lastOffset.Store(-1)
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	consumeDone := make(chan error, 1)
	go func() {
		for consumeCtx.Err() == nil {
			fetches := consumer.PollRecords(consumeCtx, 2000)
			for _, fetchErr := range fetches.Errors() {
				if !errors.Is(fetchErr.Err, context.Canceled) {
					consumeDone <- fetchErr.Err
					return
				}
			}
			for _, record := range fetches.Records() {
				if record.Attrs.IsControl() {
					lastOffset.Store(record.Offset)
					continue
				}
				var envelope struct{ Payload string }
				var event struct {
					Op    string `json:"op"`
					After struct {
						ID      int64  `json:"id"`
						Sent    int64  `json:"sent_ns"`
						Payload string `json:"payload"`
					} `json:"after"`
				}
				if *wire && len(record.Value) >= 16 {
					event.Op = "INSERT"
					event.After.ID = int64(binary.BigEndian.Uint64(record.Value[:8]))
					event.After.Sent = int64(binary.BigEndian.Uint64(record.Value[8:16]))
					event.After.Payload = string(record.Value[16:])
					if string(record.Key) != strconv.FormatInt(event.After.ID, 10) || len(record.Headers) != 1 || record.Headers[0].Key != "id" ||
						string(record.Headers[0].Value) != fmt.Sprintf("00000000-0000-0000-0000-%012x", event.After.ID) {
						consumeDone <- errors.New("wire key/identity/header changed")
						return
					}
				} else if *wire || json.Unmarshal(record.Value, &envelope) != nil || json.Unmarshal([]byte(envelope.Payload), &event) != nil || event.Op != "INSERT" {
					consumeDone <- errors.New("invalid decoded event")
					return
				}
				value := received{fmt.Sprintf("%x", sha256.Sum256([]byte(event.After.Payload))), event.After.Sent,
					float64(time.Now().UnixNano()-event.After.Sent) / 1e6}
				mu.Lock()
				if old, ok := seen[event.After.ID]; ok {
					duplicates++
					if old.checksum != value.checksum || old.sent != value.sent {
						mu.Unlock()
						consumeDone <- errors.New("duplicate changed payload")
						return
					}
				} else {
					seen[event.After.ID] = value
					count.Add(1)
				}
				mu.Unlock()
				lastOffset.Store(record.Offset)
			}
		}
		consumeDone <- nil
	}()
	var writes []float64
	var admission []float64
	produce := func(length time.Duration) error {
		if *rate == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(length):
				return nil
			}
		}
		start := time.Now()
		target := int64(float64(*rate) * length.Seconds())
		batch := max(1, *rate/100) // Ten-millisecond scheduled admission windows.
		for emitted := int64(0); emitted < target; {
			scheduled := start.Add(time.Duration(float64(emitted) / float64(*rate) * float64(time.Second)))
			if wait := time.Until(scheduled); wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
			}
			admission = append(admission, float64(time.Since(scheduled))/float64(time.Millisecond))
			n := min(batch, int(target-emitted))
			ids, times, bodies := make([]int64, n), make([]int64, n), make([]string, n)
			for i := range ids {
				id++
				ids[i] = id
				bodies[i] = synthetic(id, *size)
			}
			began := time.Now()
			for i := range times {
				times[i] = began.UnixNano()
			}
			if err := write(ctx, e.DB, ids, times, bodies); err != nil {
				return err
			}
			writes = append(writes, float64(time.Since(began))/float64(time.Millisecond))
			emitted += int64(n)
		}
		return nil
	}
	if err = produce(*warmup); err != nil {
		fatal(err)
	}
	warmupCount := id
	if *backlog > 0 {
		warmupCount = 0
	}
	writes, admission = nil, nil
	var held *pgx.Conn
	if *hold {
		held, err = pgx.Connect(ctx, e.URL)
		if err != nil {
			fatal(err)
		}
		defer held.Close(context.Background())
		if _, err = held.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM items"); err != nil {
			fatal(err)
		}
	}
	containers := []string{"rowrelay-lab-postgres-1", "rowrelay-lab-kafka-1"}
	if *mode != "control" {
		containers = append(containers, container)
	}
	paths := map[string]string{}
	for _, name := range containers {
		path, pathErr := cgroup(ctx, name)
		if pathErr != nil {
			fatal(pathErr)
		}
		paths[name] = path
	}
	start := time.Now()
	if *backlog > 0 {
		start = launchStart
	}
	samples := map[string][]sample{}
	var diagnosticSamples []map[string]any
	var diagnosticLog *os.File
	if *diagnostics {
		diagnosticLog, err = os.Create(*output + ".diagnostics.jsonl")
		if err != nil {
			fatal(err)
		}
		defer diagnosticLog.Close()
	}
	var sampleErr error
	sampleCtx, stopSamples := context.WithCancel(ctx)
	sampleDone := make(chan struct{})
	defer func() { stopSamples(); <-sampleDone }()
	go func() {
		defer close(sampleDone)
		for {
			for name, path := range paths {
				value, err := readSample(path, time.Since(start).Seconds())
				if err != nil {
					sampleErr = err
					return
				}
				samples[name] = append(samples[name], value)
			}
			if *diagnostics {
				d := map[string]any{"elapsed_s": time.Since(start).Seconds(), "decoded": count.Load()}
				var waits []byte
				if err := e.Admin.QueryRow(ctx, `SELECT coalesce(json_agg(x),'[]'::json) FROM (
					SELECT wait_event_type, wait_event, extract(epoch FROM clock_timestamp()-query_start) AS age_s,
					left(query,120) AS query FROM pg_stat_activity
					WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid()) x`).Scan(&waits); err != nil {
					sampleErr = err
					return
				}
				d["waits"] = json.RawMessage(waits)
				if *mode == "pgque" {
					var lag []byte
					if err := e.Admin.QueryRow(ctx, `SELECT row_to_json(x) FROM (
						SELECT (SELECT max(tick_id) FROM pgque.tick)-s.sub_last_tick AS pending_ticks,
						extract(epoch FROM clock_timestamp()-t.tick_time) AS consumer_lag_s,
						extract(epoch FROM clock_timestamp()-(SELECT max(tick_time) FROM pgque.tick)) AS ticker_lag_s,
						q.queue_cur_table AS current_table, q.queue_switch_time AS rotation_at
						FROM pgque.subscription s JOIN pgque.tick t ON t.tick_id=s.sub_last_tick AND t.tick_queue=s.sub_queue
						JOIN pgque.queue q ON q.queue_id=s.sub_queue) x`).Scan(&lag); err != nil {
						sampleErr = err
						return
					}
					d["batch_lag"] = json.RawMessage(lag)
				}
				if len(diagnosticSamples)%30 == 0 {
					var storage []byte
					if err := e.Admin.QueryRow(ctx, `SELECT row_to_json(x) FROM (
						SELECT coalesce(sum(pg_total_relation_size(relid)),0)::bigint AS queue_bytes,
						coalesce(sum(n_dead_tup),0)::bigint AS dead_tuples_estimated
						FROM pg_stat_user_tables WHERE schemaname IN ('pgque','pgboss')) x`).Scan(&storage); err != nil {
						sampleErr = err
						return
					}
					d["queue_storage"] = json.RawMessage(storage)
				}
				diagnosticSamples = append(diagnosticSamples, d)
				if err := json.NewEncoder(diagnosticLog).Encode(d); err != nil {
					sampleErr = err
					return
				}
			}
			select {
			case <-sampleCtx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	if *backlog > 0 {
		for count.Load() < id && ctx.Err() == nil {
			select {
			case err = <-consumeDone:
				fatal(fmt.Errorf("backlog consumer stopped: %w", err))
			case <-ctx.Done():
				fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	} else {
		if err = produce(*duration); err != nil {
			fatal(err)
		}
	}
	producerElapsed := time.Since(start)
	stopSamples()
	<-sampleDone
	if sampleErr != nil {
		fatal(sampleErr)
	}
	for name, path := range paths {
		value, err := readSample(path, producerElapsed.Seconds())
		if err != nil {
			fatal(err)
		}
		samples[name] = append(samples[name], value)
	}
	if held != nil {
		if _, err = held.Exec(ctx, "ROLLBACK"); err != nil {
			fatal(err)
		}
	}
	drainStart := time.Now()
	acknowledgedAll := *mode == "control"
	if *mode != "control" {
		for count.Load() < id && time.Since(drainStart) < time.Minute {
			select {
			case err = <-consumeDone:
				fatal(fmt.Errorf("consumer stopped: %w", err))
			case <-ctx.Done():
				fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		// Consumer receipt can precede the source ACK. Wait for the source batch
		// to finish, then read through Kafka's final high watermark before stopping.
		for ctx.Err() == nil && time.Since(drainStart) < time.Minute {
			var acknowledged bool
			query := "SELECT NOT EXISTS (SELECT FROM pgque.subscription WHERE sub_batch IS NOT NULL)"
			if *mode == "pgboss" {
				query = "SELECT NOT EXISTS (SELECT FROM pgboss.job WHERE name='events' AND state <> 'completed')"
			}
			if err = e.DB.QueryRow(ctx, query).Scan(&acknowledged); err != nil {
				fatal(err)
			}
			if acknowledged {
				ends, err := kadm.NewClient(e.Kafka).ListEndOffsets(ctx, e.Name)
				if err != nil || ends[e.Name][0].Err != nil {
					fatal(errors.New("cannot obtain final Kafka high watermark"))
				}
				if lastOffset.Load()+1 == ends[e.Name][0].Offset {
					acknowledgedAll = true
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	recovery := time.Since(drainStart)
	consumeCancel()
	if err = <-consumeDone; err != nil {
		fatal(err)
	}
	latencies := make([]float64, 0, len(seen))
	missing, corrupt, expected := 0, 0, 0
	if !acknowledgedAll {
		corrupt++
	}
	rows, err := e.DB.Query(ctx, "SELECT id, sent_ns, checksum FROM expected ORDER BY id")
	if err != nil {
		fatal(err)
	}
	for rows.Next() {
		var key, sent int64
		var checksum string
		if err = rows.Scan(&key, &sent, &checksum); err != nil {
			fatal(err)
		}
		expected++
		if *mode == "control" {
			continue
		}
		v, ok := seen[key]
		if !ok {
			missing++
		} else if v.sent != sent || v.checksum != checksum {
			corrupt++
		} else if key > warmupCount {
			latencies = append(latencies, v.latency)
		}
		delete(seen, key)
	}
	if err = rows.Err(); err != nil {
		fatal(err)
	}
	rows.Close()
	var queueBytes, dead int64
	if err = e.Admin.QueryRow(ctx, `SELECT coalesce(sum(pg_total_relation_size(relid)),0)::bigint,
		coalesce(sum(n_dead_tup),0)::bigint FROM pg_stat_user_tables WHERE schemaname IN ('rowrelay','rowrelay_outbox','pgque','pgboss')`).Scan(&queueBytes, &dead); err != nil {
		fatal(err)
	}
	var calls int64
	var sqlMS float64
	if err = e.Admin.QueryRow(ctx, `SELECT coalesce(sum(calls),0)::bigint, coalesce(sum(total_exec_time),0)
		FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&calls, &sqlMS); err != nil {
		fatal(err)
	}
	var statements []byte
	if err = e.Admin.QueryRow(ctx, `SELECT coalesce(json_agg(x),'[]'::json) FROM (
		SELECT left(query,400) AS query,calls,total_exec_time,mean_exec_time,max_exec_time,shared_blks_hit,shared_blks_read
		FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
		ORDER BY total_exec_time DESC LIMIT 20) x`).Scan(&statements); err != nil {
		fatal(err)
	}
	var throttled = map[string]string{}
	for name, path := range paths {
		data, _ := os.ReadFile(filepath.Join(path, "cpu.stat"))
		throttled[name] = string(data)
	}
	result := map[string]any{
		"mode": *mode, "rate": *rate, "duration_s": duration.Seconds(), "warmup_s": warmup.Seconds(),
		"business_outbox": *wire, "prepared_backlog": *backlog,
		"node_ingress":  *nodeIngress,
		"payload_bytes": *size, "hold_xmin": *hold, "compression": *compression,
		"poll_ms":            float64(*poll) / float64(time.Millisecond),
		"producer_elapsed_s": producerElapsed.Seconds(), "recovery_s": recovery.Seconds(),
		"committed": expected, "measured_committed": id - warmupCount, "missing": missing, "corrupt": corrupt, "phantoms": len(seen), "duplicates": duplicates,
		"source_ack_complete":           acknowledgedAll,
		"latency_tx_start_to_decode_ms": distribution(latencies), "write_tx_ms": distribution(writes), "admission_delay_ms": distribution(admission),
		"queue_bytes": queueBytes, "dead_tuples_estimated": dead, "db_statement_calls": calls, "db_statement_exec_ms": sqlMS,
		"samples": samples, "cpu_stat_end": throttled, "go": runtime.Version(), "timestamp": time.Now().UTC(),
		"diagnostics_enabled": *diagnostics, "diagnostics": diagnosticSamples, "top_statements": json.RawMessage(statements),
		"source": e.Name, "broker_replication_factor": 1, "min_isr": 1, "acks": "all", "idempotent": true,
		"transactional": *mode != "control", "consumer_isolation": "read_committed",
		"relay_cpu_limit": 1, "relay_memory_limit_mib": 128, "gomemlimit_mib": 96,
		"pgque_commit": "e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6", "seed": 42,
	}
	if *nodeIngress {
		var factBytes int64
		if err = e.DB.QueryRow(ctx, "SELECT pg_total_relation_size('bench_fact')").Scan(&factBytes); err != nil {
			fatal(err)
		}
		result["immutable_fact_bytes"] = factBytes
		result["ingress_node"], err = command(ctx, "node", "--version")
		if err != nil {
			fatal(err)
		}
	}
	if *mode == "pgboss" {
		delete(result, "gomemlimit_mib")
		result["node_old_space_limit_mib"] = 96
		result["pgboss_version"], result["kafkajs_version"] = "12.33.2", "2.2.4"
		result["baseline_scheduler"] = "official fetch/complete API with explicit polling"
		result["relay_image"], err = command(ctx, "docker", "inspect", "-f", "{{.Image}}", container)
		if err != nil {
			fatal(err)
		}
	}
	if *mode != "control" {
		log, logErr := command(ctx, "docker", "logs", container)
		result["relay_log"] = log
		state, stateErr := command(ctx, "docker", "inspect", "-f", "{{.State.Running}} {{.State.OOMKilled}}", container)
		result["relay_state"] = strings.TrimSpace(state)
		peak, peakErr := os.ReadFile(filepath.Join(paths[container], "memory.peak"))
		if peakErr == nil {
			bytes, parseErr := strconv.ParseFloat(strings.TrimSpace(string(peak)), 64)
			if parseErr == nil {
				result["relay_memory_peak_since_start_mib"] = bytes / (1 << 20)
			}
		}
		if logErr != nil || stateErr != nil || strings.TrimSpace(state) != "true false" {
			corrupt++
			result["corrupt"] = corrupt
		}
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		fatal(err)
	}
	if err = os.WriteFile(*output, append(data, '\n'), 0644); err != nil {
		fatal(err)
	}
	fmt.Printf("%s: committed=%d missing=%d corrupt=%d phantoms=%d duplicates=%d result=%s\n", *mode, expected, missing, corrupt, len(seen), duplicates, *output)
	if missing != 0 || corrupt != 0 || len(seen) != 0 {
		fatal(errors.New("correctness reconciliation failed"))
	}
}

func synthetic(id int64, size int) string {
	rng := rand.New(rand.NewPCG(uint64(id), 42))
	data := make([]byte, (size*3+3)/4)
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	return base64.StdEncoding.EncodeToString(data)[:size]
}

func distribution(values []float64) map[string]float64 {
	if len(values) == 0 {
		return nil
	}
	sort.Float64s(values)
	return map[string]float64{"p50": values[(len(values)-1)*50/100], "p95": values[(len(values)-1)*95/100],
		"p99": values[(len(values)-1)*99/100], "max": values[len(values)-1]}
}

func command(ctx context.Context, name string, args ...string) (string, error) {
	data, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(data), fmt.Errorf("%s failed: %w: %s", name, err, data)
	}
	return string(data), nil
}

func cgroup(ctx context.Context, name string) (string, error) {
	pid, err := command(ctx, "docker", "inspect", "-f", "{{.State.Pid}}", name)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile("/proc/" + strings.TrimSpace(pid) + "/cgroup")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::/") {
			return filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::")), nil
		}
	}
	return "", errors.New("cgroup v2 required for resource accounting")
}

func readSample(path string, elapsed float64) (sample, error) {
	stat := func(file, key string) (float64, error) {
		data, err := os.ReadFile(filepath.Join(path, file))
		if err != nil {
			return 0, err
		}
		if key == "" {
			return strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
		}
		fields := strings.Fields(string(data))
		for i := 0; i+1 < len(fields); i += 2 {
			if fields[i] == key {
				return strconv.ParseFloat(fields[i+1], 64)
			}
		}
		return 0, fmt.Errorf("missing %s in %s", key, file)
	}
	cpu, err := stat("cpu.stat", "usage_usec")
	if err != nil {
		return sample{}, err
	}
	memory, err := stat("memory.current", "")
	if err != nil {
		return sample{}, err
	}
	anon, err := stat("memory.stat", "anon")
	value := sample{At: elapsed, CPUSeconds: cpu / 1e6, MemoryMiB: memory / (1 << 20), AnonymousMiB: anon / (1 << 20)}
	// A single-process container has an unambiguous RSS. Do not sum PostgreSQL
	// backend RSS, which would count shared mappings repeatedly.
	if pids, readErr := os.ReadFile(filepath.Join(path, "cgroup.procs")); readErr == nil {
		if fields := strings.Fields(string(pids)); len(fields) == 1 {
			if status, readErr := os.ReadFile("/proc/" + fields[0] + "/status"); readErr == nil {
				for _, line := range strings.Split(string(status), "\n") {
					if strings.HasPrefix(line, "VmRSS:") {
						if kb, parseErr := strconv.ParseFloat(strings.Fields(line)[1], 64); parseErr == nil {
							mib := kb / 1024
							value.ProcessRSSMiB = &mib
						}
					}
				}
			}
		}
	}
	return value, err
}

func fatal(err error) {
	// Panic runs deferred cleanup; this disposable harness uses only local lab credentials.
	panic(err)
}
