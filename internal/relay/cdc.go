package relay

import (
	"errors"

	"github.com/sleepkqq/row-relay/internal/cdcwire"
)

func (r *Runner) encodeCDC(topic string, id int64, change Change) ([]byte, error) {
	if id < 1 {
		return nil, errors.New("invalid source record")
	}
	before, err := cdcwire.Image(change.Before)
	if err != nil {
		return nil, err
	}
	after, err := cdcwire.Image(change.After)
	if err != nil {
		return nil, err
	}
	operation := cdcwire.RowChange_Operation(cdcwire.RowChange_Operation_value["OPERATION_"+change.Op])
	return cdcwire.Encode(r.schemaIDs[topic], &cdcwire.CacheCdcRecord{
		Version: 1, SourceEpoch: r.epoch,
		Body: &cdcwire.CacheCdcRecord_Change{Change: &cdcwire.RowChange{
			EventId: id, Schema: change.Schema, Table: change.Table,
			Operation: operation, Before: before, After: after,
		}},
	})
}
