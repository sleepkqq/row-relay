package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Stream struct {
	Name   string
	Config Config
}

// LoadStreams accepts explicit routes and environment-variable references, never
// credentials in the configuration file or destinations selected by event data.
func LoadStreams(reader io.Reader, base Config, lookup func(string) (string, bool)) ([]Stream, error) {
	var file struct {
		Streams []struct {
			Name                string            `json:"name"`
			DatabaseEnv         string            `json:"database_env"`
			Topic               string            `json:"topic"`
			SchemaTopics        map[string]string `json:"schema_topics"`
			CDCFormat           string            `json:"cdc_format"`
			OutboxStream        string            `json:"outbox_stream"`
			Batch               int               `json:"batch"`
			BatchBytes          int               `json:"batch_bytes"`
			ProgressInterval    string            `json:"progress_interval"`
			DeliveryMode        string            `json:"delivery_mode"`
			ManagedTakeover     *bool             `json:"managed_takeover"`
			ManagedInvalidation *bool             `json:"managed_invalidation"`
		} `json:"streams"`
	}
	data, err := io.ReadAll(io.LimitReader(reader, 65537))
	if err != nil || len(data) > 65536 {
		return nil, errors.New("cannot read stream configuration within 64 KiB limit")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&file); err != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid stream configuration")
	}
	if len(file.Streams) == 0 || len(file.Streams) > 16 {
		return nil, errors.New("configuration requires 1..16 streams")
	}
	namePattern := regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	seen, sources := map[string]bool{}, map[string]bool{}
	streams := make([]Stream, 0, len(file.Streams))
	totalBytes := 0
	for _, source := range file.Streams {
		if !namePattern.MatchString(source.Name) || seen[source.Name] || source.DatabaseEnv == "" {
			return nil, errors.New("invalid or duplicate stream name/database reference")
		}
		seen[source.Name] = true
		c := base
		var ok bool
		c.DatabaseURL, ok = lookup(source.DatabaseEnv)
		if !ok || c.DatabaseURL == "" {
			return nil, errors.New("stream database environment variable is missing")
		}
		c.Topic, c.OutboxStream = source.Topic, source.OutboxStream
		c.SchemaTopics = source.SchemaTopics
		if source.CDCFormat != "" {
			c.CDCFormat = source.CDCFormat
		}
		if source.DeliveryMode != "" {
			c.DeliveryMode = source.DeliveryMode
		}
		if source.ManagedTakeover != nil {
			c.ManagedTakeover = *source.ManagedTakeover
		}
		if source.ManagedInvalidation != nil {
			c.ManagedInvalidation = *source.ManagedInvalidation
		}
		if source.Batch != 0 {
			c.Batch = source.Batch
		}
		if source.BatchBytes != 0 {
			c.BatchBytes = source.BatchBytes
		}
		if source.ProgressInterval != "" {
			c.ProgressInterval, err = time.ParseDuration(source.ProgressInterval)
			if err != nil {
				return nil, errors.New("invalid progress interval")
			}
		}
		if err = c.Validate(); err != nil {
			return nil, err
		}
		key := c.DatabaseURL + "\x00" + c.OutboxStream
		if sources[key] {
			return nil, errors.New("duplicate source registration")
		}
		sources[key] = true
		totalBytes += c.BatchBytes
		streams = append(streams, Stream{Name: source.Name, Config: c})
	}
	if totalBytes > 32<<20 {
		return nil, fmt.Errorf("stream batch byte budgets total %d MiB (%d bytes); maximum is 32 MiB", totalBytes>>20, totalBytes)
	}
	return streams, nil
}

// RunStreams uses one bounded producer per independently owned stream and
// capped reopen backoff per stream. A failed stream never advances its source.
// report is serialized; nil reports a completed source step (including idle),
// never source freshness.
// Only fenced mode supports a paused predecessor resuming after takeover.
func RunStreams(ctx context.Context, streams []Stream, report func(string, error)) error {
	return RunStreamsActivity(ctx, streams, report, nil)
}

// RunStreamsActivity additionally reports real per-stream source activity to a
// callback serialized with report by the same mutex. activity is an operability
// signal only; it is never a freshness proof and a stream never advances its
// source because of it.
func RunStreamsActivity(ctx context.Context, streams []Stream, report func(string, error), activity func(string)) error {
	if len(streams) == 0 || len(streams) > 16 {
		return errors.New("invalid stream count")
	}
	producerConfig := streams[0].Config
	producerConfig.Batch, producerConfig.BatchBytes = 0, 0
	for _, stream := range streams {
		c := stream.Config
		if err := c.Validate(); err != nil {
			return err
		}
		if strings.Join(c.Brokers, "\x00") != strings.Join(producerConfig.Brokers, "\x00") ||
			c.Compression != producerConfig.Compression || c.Timeout != producerConfig.Timeout || c.Security != producerConfig.Security {
			return errors.New("shared streams require the same Kafka transport settings")
		}
		producerConfig.Batch += c.Batch
		producerConfig.BatchBytes += c.BatchBytes
	}
	if producerConfig.BatchBytes > 32<<20 {
		return errors.New("aggregate stream buffer budget exceeded")
	}
	var workers sync.WaitGroup
	var reports sync.Mutex
	notify := func(name string, err error) {
		if report != nil {
			reports.Lock()
			defer reports.Unlock()
			report(name, err)
		}
	}
	touch := func(name string) {
		if activity != nil {
			reports.Lock()
			defer reports.Unlock()
			activity(name)
		}
	}
	for _, stream := range streams {
		workers.Go(func() {
			backoff := time.Second
			for ctx.Err() == nil {
				started := time.Now()
				openCtx, cancel := context.WithTimeout(ctx, stream.Config.Timeout)
				runner, err := Open(openCtx, stream.Config)
				cancel()
				if err == nil {
					err = runner.RunObservedActivity(ctx, func() { notify(stream.Name, nil) }, func() { touch(stream.Name) })
					if ctx.Err() == nil {
						notify(stream.Name, err)
					}
					runner.Close()
				} else {
					notify(stream.Name, err)
				}
				if ctx.Err() != nil {
					return
				}
				if time.Since(started) > 10*time.Second {
					backoff = time.Second
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(5*time.Second, 2*backoff)
			}
		})
	}
	workers.Wait()
	return nil
}
