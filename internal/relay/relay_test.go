package relay

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestEnvelopePreservesNumbersAndStableIdentity(t *testing.T) {
	raw := []byte(`{"schema":"public","table":"item","op":"UPDATE","before":{"id":9007199254740993,"fk":1,"amount":12345678901234567890.12345},"after":{"id":9007199254740993,"fk":null,"amount":12345678901234567890.12345}}`)
	encoded, err := Encode("epoch", 7, raw)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Headers map[string]string `json:"headers"`
		Payload string            `json:"payload"`
	}
	if err := json.Unmarshal(encoded, &event); err != nil || event.Payload != string(raw) || event.Headers["ID"] != "epoch:7" {
		t.Fatalf("changed identity or payload: %s; %v", encoded, err)
	}
	other, _ := Encode("epoch", 7, raw)
	if !bytes.Equal(encoded, other) {
		t.Fatal("retry changed event identity")
	}
}

func TestPoisonDoesNotLeakRowData(t *testing.T) {
	for _, raw := range []string{
		`secret-invalid-json`,
		`{"schema":"public","table":"item","op":"DELETE","after":{"secret":1}}`,
		`{"schema":"public","table":"item","op":"UPDATE","before":null,"after":{"secret":1}}`,
		`{"schema":"public","table":"item","op":"TRUNCATE","before":{"secret":1}}`,
	} {
		_, err := Encode("epoch", 1, []byte(raw))
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("poison accepted or exposed: %v", err)
		}
	}
}
