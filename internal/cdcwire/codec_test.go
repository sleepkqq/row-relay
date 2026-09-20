package cdcwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestWirePreservesExactValuesPresenceAndReplay(t *testing.T) {
	image, err := Image([]byte(`{"id":9223372036854775807,"decimal":12345678901234567890.00100,"exponent":1e-400,"null":null,"empty":"","false":false,"bytes":"\\x00ff","array":[null,{},[],0]}`))
	if err != nil {
		t.Fatal(err)
	}
	record := &CacheCdcRecord{Version: 1, SourceEpoch: "epoch", Body: &CacheCdcRecord_Change{Change: &RowChange{
		EventId: 1, Schema: "fixture", Table: "items", Operation: RowChange_OPERATION_INSERT, After: image,
	}}}
	wire, err := Encode(123, record)
	if err != nil {
		t.Fatal(err)
	}
	if wire[0] != 0 || binary.BigEndian.Uint32(wire[1:5]) != 123 || wire[5] != 0 {
		t.Fatal("invalid Confluent frame")
	}
	var decoded CacheCdcRecord
	if err = proto.Unmarshal(wire[6:], &decoded); err != nil {
		t.Fatal(err)
	}
	change := decoded.GetChange()
	if change.Before != nil || change.After == nil {
		t.Fatal("row-image presence changed")
	}
	fields := change.After.Fields
	for field, value := range map[string]string{"id": "9223372036854775807", "decimal": "12345678901234567890.00100", "exponent": "1e-400"} {
		if fields[field].GetNumberText() != value {
			t.Fatal("number changed", field)
		}
	}
	if _, ok := fields["null"].Kind.(*ColumnValue_NullValue); !ok || fields["missing"] != nil {
		t.Fatal("NULL and absence conflated")
	}
	if _, ok := fields["empty"].Kind.(*ColumnValue_StringValue); !ok {
		t.Fatal("empty string lost")
	}
	if _, ok := fields["false"].Kind.(*ColumnValue_BoolValue); !ok {
		t.Fatal("false lost")
	}
	if fields["bytes"].GetStringValue() != `\x00ff` || len(fields["array"].GetArrayValue().Values) != 4 {
		t.Fatal("bytes/array changed")
	}
	for range 20 {
		again, err := Encode(123, record)
		if err != nil || !bytes.Equal(wire, again) {
			t.Fatal("replay changed wire bytes", err)
		}
	}
	for _, invalid := range []string{`[]`, `{"secret":`, `{} {}`, strings.Repeat(`{"x":`, 66) + `null` + strings.Repeat(`}`, 66)} {
		if _, err := Image([]byte(invalid)); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("invalid image accepted or payload leaked")
		}
	}
}

func TestLookupRequiresRegisteredContractForEveryTopic(t *testing.T) {
	status := http.StatusOK
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || strings.HasSuffix(r.URL.Path, "/versions") {
			t.Error("lookup must never register")
		}
		var body map[string]string
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["schema"] != Schema || body["schemaType"] != "PROTOBUF" {
			t.Error("contract mismatch")
		}
		seen[r.URL.Path] = true
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"id":7}`))
	}))
	defer server.Close()
	topics := []string{"catalog.cache-cdc", "orders.cache-cdc", "inventory.cache-cdc"}
	ids, err := Lookup(context.Background(), server.URL, topics)
	if err != nil || len(ids) != 3 || len(seen) != 3 {
		t.Fatal(ids, err)
	}
	for _, topic := range topics {
		if ids[topic] != 7 || !seen["/subjects/"+topic+"-value"] {
			t.Fatal("missing subject validation")
		}
	}
	status = http.StatusNotFound
	if _, err = Lookup(context.Background(), server.URL, topics); err == nil {
		t.Fatal("unregistered schema accepted")
	}
	server.Close()
	if _, err = Lookup(context.Background(), server.URL, topics); err == nil {
		t.Fatal("cold unavailable registry accepted")
	}
}
