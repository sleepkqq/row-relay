package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type ingress struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	encode *json.Encoder
	decode *json.Decoder
	log    *os.File
}

type ingressRecord struct {
	ID         string `json:"id"`
	Sent       string `json:"sent_ns"`
	Payload    string `json:"payload"`
	Checksum   string `json:"checksum"`
	DeliveryID string `json:"delivery_id"`
	Key        []byte `json:"kafka_key"`
	Value      []byte `json:"kafka_value"`
}

func startIngress(ctx context.Context, url, mode, directory, output string) (*ingress, error) {
	log, err := os.Create(output + ".ingress.log")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "node", filepath.Join(directory, "ingress.mjs"))
	cmd.Env = append(os.Environ(), "DATABASE_URL="+url, "BASELINE_MODE="+mode)
	cmd.Stderr = log
	input, err := cmd.StdinPipe()
	if err != nil {
		log.Close()
		return nil, err
	}
	outputPipe, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		log.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		input.Close()
		outputPipe.Close()
		log.Close()
		return nil, err
	}
	i := &ingress{cmd, input, json.NewEncoder(input), json.NewDecoder(outputPipe), log}
	var ready struct{ Ready bool }
	if err = i.decode.Decode(&ready); err != nil || !ready.Ready {
		i.Close()
		return nil, fmt.Errorf("Node ingress startup failed; inspect %s.ingress.log", output)
	}
	return i, nil
}

func (i *ingress) Write(ctx context.Context, _ *pgx.Conn, ids, sent []int64, payloads []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) == 0 || len(ids) > 1000 || len(sent) != len(ids) || len(payloads) != len(ids) {
		return errors.New("invalid ingress batch")
	}
	records := make([]ingressRecord, len(ids))
	for j, id := range ids {
		wire := make([]byte, 16+len(payloads[j]))
		binary.BigEndian.PutUint64(wire, uint64(id))
		binary.BigEndian.PutUint64(wire[8:], uint64(sent[j]))
		copy(wire[16:], payloads[j])
		records[j] = ingressRecord{
			ID: strconv.FormatInt(id, 10), Sent: strconv.FormatInt(sent[j], 10), Payload: payloads[j],
			Checksum:   fmt.Sprintf("%x", sha256.Sum256([]byte(payloads[j]))),
			DeliveryID: fmt.Sprintf("00000000-0000-0000-0000-%012x", id),
			Key:        []byte(strconv.FormatInt(id, 10)), Value: wire,
		}
	}
	if err := i.encode.Encode(records); err != nil {
		return err
	}
	var result struct{ Committed int }
	if err := i.decode.Decode(&result); err != nil {
		return err
	}
	if result.Committed != len(ids) {
		return errors.New("incomplete ingress commit")
	}
	return nil
}

func (i *ingress) Close() {
	i.input.Close()
	done := make(chan struct{})
	go func() { _ = i.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = i.cmd.Process.Kill()
		<-done
	}
	i.log.Close()
}
