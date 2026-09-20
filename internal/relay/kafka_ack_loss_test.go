//go:build integration

package relay

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// kafkaCommitReplyLoss cuts actual EndTxn responses and all subsequent
// connections. The caller verifies commitment through a read_committed reader;
// a source ACK must never follow the producer's unconfirmed commit result.
func kafkaCommitReplyLoss(t *testing.T, ctx context.Context, upstream string) (func(context.Context, string, string) (net.Conn, error), <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var cut atomic.Bool
	fired := make(chan struct{})
	var workers sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() { cancel(); listener.Close(); workers.Wait() })
	workers.Go(func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			if cut.Load() {
				client.Close()
				continue
			}
			workers.Go(func() {
				defer client.Close()
				server, err := (&net.Dialer{}).DialContext(ctx, "tcp", upstream)
				if err != nil {
					return
				}
				defer server.Close()
				stop := context.AfterFunc(ctx, func() { client.Close(); server.Close() })
				defer stop()
				var mu sync.Mutex
				requests := map[uint32]uint16{}
				forwarded := make(chan struct{})
				go func() {
					defer close(forwarded)
					defer server.Close()
					for {
						frame, err := kafkaFrame(client)
						if err != nil || len(frame) < 12 {
							return
						}
						mu.Lock()
						requests[binary.BigEndian.Uint32(frame[8:12])] = binary.BigEndian.Uint16(frame[4:6])
						mu.Unlock()
						if _, err = server.Write(frame); err != nil {
							return
						}
					}
				}()
				defer func() { client.Close(); server.Close(); <-forwarded }()
				for {
					frame, err := kafkaFrame(server)
					if err != nil || len(frame) < 8 {
						return
					}
					id := binary.BigEndian.Uint32(frame[4:8])
					mu.Lock()
					api := requests[id]
					delete(requests, id)
					mu.Unlock()
					if api == 26 {
						if cut.CompareAndSwap(false, true) {
							close(fired)
						}
						return
					}
					if _, err = client.Write(frame); err != nil {
						return
					}
				}
			})
		}
	})
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if cut.Load() {
			return nil, errors.New("injected Kafka commit response loss")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}, fired
}

func kafkaFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size < 4 || size > 16<<20 {
		return nil, io.ErrUnexpectedEOF
	}
	frame := make([]byte, size+4)
	copy(frame, header[:])
	_, err := io.ReadFull(r, frame[4:])
	return frame, err
}
