package relay

import (
	"errors"
	"strings"
	"testing"
)

func TestOperationErrorHidesCauseAndRetainsClassification(t *testing.T) {
	cause := errors.New("dsn=secret rows=private-payload")
	err := wrapOperation(opKafkaPublish, cause)
	if err.Error() != "Kafka publish failed" {
		t.Fatalf("unexpected public message: %q", err.Error())
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "dsn") || strings.Contains(err.Error(), "private") {
		t.Fatal("operation error exposed its cause")
	}
	if !errors.Is(err, cause) {
		t.Fatal("operation error dropped its cause")
	}
	var opErr *OperationError
	if !errors.As(err, &opErr) || opErr.Operation != "Kafka publish" || !errors.Is(opErr.Err, cause) {
		t.Fatal("operation error is not classifiable")
	}
	if wrapOperation(opSourceAck, nil) != nil {
		t.Fatal("nil cause produced an operation error")
	}
}

func TestOperationLabelsAreFixedPhrases(t *testing.T) {
	for _, op := range []operationLabel{
		opSourceConnect, opSourceOwnership, opSourceSession, opSourceRegistration,
		opRegistryLookup, opProducerInit, opTopicMetadata, opSourceFetch,
		opSourceDecode, opRecordEncode, opRecordOversize, opKafkaPublish, opSourceAck,
	} {
		if string(op) == "" || strings.ContainsAny(string(op), "\n\r\t") {
			t.Fatalf("operation label is not a fixed phrase: %q", op)
		}
	}
}
