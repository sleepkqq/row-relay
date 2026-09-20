package outbox

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestWireBytesHeadersAndIdentity(t *testing.T) {
	m := Message{ID: "11111111-2222-3333-4444-555555555555", Key: []byte{0, 255, 1}, Value: []byte{0, 0, 0, 1, 128, 255}}
	m.Headers = []Header{{"id", []byte(m.ID)}, {"binary", []byte{0, 255}}, {"binary", nil}}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Decode(raw)
	if err != nil || !bytes.Equal(r.Key, m.Key) || !bytes.Equal(r.Value, m.Value) || len(r.Headers) != 3 ||
		!bytes.Equal(r.Headers[1].Value, m.Headers[1].Value) || r.Headers[2].Value != nil {
		t.Fatalf("wire record changed: %v", err)
	}
	m.Headers[0].Value = []byte("wrong identity")
	raw, _ = json.Marshal(m)
	if _, err = Decode(raw); err == nil {
		t.Fatal("accepted inconsistent delivery identity")
	}
	if _, err = Decode([]byte(`{"value":"private payload"}`)); err == nil || err.Error() != "invalid business-outbox record" {
		t.Fatal("poison accepted or leaked")
	}
}
