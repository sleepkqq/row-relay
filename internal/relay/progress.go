package relay

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sleepkqq/row-relay/internal/cdcwire"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Progress certifies all captured commits visible before CommittedBefore, not
// all transactions started before that instant and not an event-ID watermark.
type Progress struct {
	Version         int       `json:"version"`
	SourceEpoch     string    `json:"source_epoch"`
	CommittedBefore time.Time `json:"committed_before"`
}

func (r *Runner) progressDue() bool {
	return r.config.ProgressInterval > 0 && time.Since(r.lastProgress) >= r.config.ProgressInterval
}

func (r *Runner) publishProgress(ctx context.Context, boundary time.Time) error {
	var owned, primary bool
	if err := r.db.QueryRow(ctx, "SELECT ("+ownershipQuery+"), NOT pg_is_in_recovery()", r.lockID).Scan(&owned, &primary); err != nil {
		return errors.New("progress ownership check failed")
	}
	if !owned || !primary {
		return errors.New("progress requires an owned primary source")
	}
	if boundary.IsZero() {
		return errors.New("missing closed source boundary")
	}
	kind := "rowrelay_progress"
	if r.config.ManagedInvalidation {
		// Late predecessors may repeat/reorder invalidations and older boundaries.
		// A distinct wire kind prevents generic materializing consumers accepting it.
		kind = "rowrelay_invalidation_progress"
	}
	value, err := json.Marshal(map[string]Progress{kind: {1, r.epoch, boundary.UTC()}})
	if err != nil {
		return errors.New("encode progress failed")
	}
	records := make([]*kgo.Record, 0, len(r.config.topics()))
	for _, topic := range r.config.topics() {
		encoded := value
		if r.config.CDCFormat != "legacy-json" {
			mode := cdcwire.ClosedBoundary_MODE_FENCED
			if r.config.ManagedInvalidation {
				mode = cdcwire.ClosedBoundary_MODE_MANAGED_INVALIDATION
			}
			timestamp := timestamppb.New(boundary.UTC())
			if timestamp.CheckValid() != nil {
				return errors.New("invalid source boundary timestamp")
			}
			encoded, err = cdcwire.Encode(r.schemaIDs[topic], &cdcwire.CacheCdcRecord{
				Version: 1, SourceEpoch: r.epoch,
				Body: &cdcwire.CacheCdcRecord_Boundary{Boundary: &cdcwire.ClosedBoundary{
					CommittedBefore: timestamp, Mode: mode,
				}},
			})
			if err != nil {
				return errors.New("encode progress failed")
			}
		}
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(r.epoch), Value: encoded})
	}
	if err = r.publish(ctx, records); err != nil {
		return err
	}
	r.lastProgress = time.Now()
	return nil
}

func (r *Runner) publish(ctx context.Context, records []*kgo.Record) error {
	if r.config.DeliveryMode != "managed" {
		if err := r.kafka.BeginTransaction(); err != nil {
			return errors.New("begin Kafka transaction failed")
		}
	}
	if err := r.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return errors.New("Kafka batch acknowledgement failed")
	}
	if r.config.DeliveryMode != "managed" {
		if err := r.kafka.EndTransaction(ctx, kgo.TryCommit); err != nil {
			// Reopen under source ownership; never reinitialize an uncertain producer.
			return errors.New("Kafka transaction commit unconfirmed")
		}
	}
	return nil
}
