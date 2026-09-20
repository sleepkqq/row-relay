// Package cdcwire encodes captured values as binary Protobuf, with exact numbers
// and explicit NULL/presence. The public schema is embedded from cache_cdc.proto.
package cdcwire

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed cache_cdc.proto
var Schema string

// Lookup verifies the exact registered contract for every output subject.
// It neither registers schemas nor falls back to a different wire format.
func Lookup(ctx context.Context, base string, topics []string) (map[string]uint32, error) {
	endpoint, err := url.Parse(base)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil {
		return nil, errors.New("invalid CDC schema registry URL")
	}
	body, _ := json.Marshal(map[string]string{"schemaType": "PROTOBUF", "schema": Schema})
	client := &http.Client{Timeout: 5 * time.Second}
	ids := make(map[string]uint32, len(topics))
	for _, topic := range topics {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			strings.TrimRight(base, "/")+"/subjects/"+url.PathEscape(topic+"-value"), bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("prepare CDC schema lookup failed")
		}
		request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		response, err := client.Do(request)
		if err != nil {
			return nil, errors.New("CDC schema registry unavailable")
		}
		var registered struct {
			ID int64 `json:"id"`
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if response.StatusCode != http.StatusOK || readErr != nil || json.Unmarshal(data, &registered) != nil || registered.ID < 1 || registered.ID > 1<<31-1 {
			return nil, errors.New("CDC Protobuf contract is not registered")
		}
		ids[topic] = uint32(registered.ID)
	}
	return ids, nil
}

func Encode(schemaID uint32, record *CacheCdcRecord) ([]byte, error) {
	if schemaID == 0 || schemaID > 1<<31-1 {
		return nil, errors.New("invalid CDC schema ID")
	}
	// Confluent magic + schema ID + optimized message-index path [0].
	header := make([]byte, 6)
	binary.BigEndian.PutUint32(header[1:5], schemaID)
	return (proto.MarshalOptions{Deterministic: true}).MarshalAppend(header, record)
}

func Image(raw []byte) (*RowImage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if decoder.Decode(&object) != nil || decoder.Decode(new(any)) != io.EOF || object == nil {
		return nil, errors.New("invalid CDC row image")
	}
	value, err := convert(object, 0)
	if err != nil {
		return nil, err
	}
	return value.GetObjectValue(), nil
}

func convert(value any, depth int) (*ColumnValue, error) {
	if depth > 30 {
		return nil, errors.New("CDC value nesting exceeds limit")
	}
	switch value := value.(type) {
	case nil:
		return &ColumnValue{Kind: &ColumnValue_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
	case bool:
		return &ColumnValue{Kind: &ColumnValue_BoolValue{BoolValue: value}}, nil
	case string:
		return &ColumnValue{Kind: &ColumnValue_StringValue{StringValue: value}}, nil
	case json.Number:
		return &ColumnValue{Kind: &ColumnValue_NumberText{NumberText: value.String()}}, nil
	case map[string]any:
		image := &RowImage{Fields: make(map[string]*ColumnValue, len(value))}
		for name, field := range value {
			converted, err := convert(field, depth+1)
			if err != nil {
				return nil, err
			}
			image.Fields[name] = converted
		}
		return &ColumnValue{Kind: &ColumnValue_ObjectValue{ObjectValue: image}}, nil
	case []any:
		list := &ValueList{Values: make([]*ColumnValue, 0, len(value))}
		for _, field := range value {
			converted, err := convert(field, depth+1)
			if err != nil {
				return nil, err
			}
			list.Values = append(list.Values, converted)
		}
		return &ColumnValue{Kind: &ColumnValue_ArrayValue{ArrayValue: list}}, nil
	default:
		return nil, errors.New("unsupported CDC value")
	}
}
