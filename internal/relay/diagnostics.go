package relay

// operationLabel names one bounded relay operation. It is a private string type
// so only the authored constants below can be wrapped: call sites cannot inject
// driver text, row data, headers, or credentials into the public label.
type operationLabel string

// Authored, constant operation labels. These are the only values that can reach
// OperationError.Operation.
const (
	opSourceConnect      operationLabel = "source connect"
	opSourceOwnership    operationLabel = "source ownership check"
	opSourceSession      operationLabel = "source session configuration"
	opSourceRegistration operationLabel = "source registration"
	opRegistryLookup     operationLabel = "schema registry lookup"
	opProducerInit       operationLabel = "Kafka producer initialization"
	opTopicMetadata      operationLabel = "output topic metadata"
	opSourceFetch        operationLabel = "source snapshot fetch"
	opSourceDecode       operationLabel = "source record decode"
	opRecordEncode       operationLabel = "record encode"
	opRecordOversize     operationLabel = "encoded record size"
	opKafkaPublish       operationLabel = "Kafka publish"
	opSourceAck          operationLabel = "source acknowledgement"
)

// OperationError classifies a relay failure without exposing the underlying
// driver, row, or endpoint text. Error renders only the fixed "operation
// failed" form; Unwrap keeps the cause available to errors.Is/errors.As.
type OperationError struct {
	Operation string
	Err       error
}

func (e *OperationError) Error() string { return e.Operation + " failed" }

func (e *OperationError) Unwrap() error { return e.Err }

// wrapOperation tags err with an authored operation label. It is the only
// constructor: operationLabel is unexported, so no caller can pass an arbitrary
// or externally sourced string.
func wrapOperation(op operationLabel, err error) error {
	if err == nil {
		return nil
	}
	return &OperationError{Operation: string(op), Err: err}
}
