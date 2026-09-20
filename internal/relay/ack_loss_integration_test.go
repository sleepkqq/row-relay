//go:build integration

package relay_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
)

// loseAckResponse forwards the actual PostgreSQL protocol. Once armed it holds
// CommandComplete for the matching statement, waits for the server's idle
// ReadyForQuery (autocommit finished), then disconnects without delivering either.
// Prepared-statement names are tracked so a cached ACK is intercepted as well.
func loseAckResponse(t *testing.T, dsn, statement string) (string, *atomic.Bool, <-chan struct{}) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := u.Host
	u.Host = listener.Addr().String()
	armed := new(atomic.Bool)
	fired, done := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); listener.Close(); <-done })
	go func() {
		defer close(done)
		client, err := listener.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		server, err := net.DialTimeout("tcp", upstream, time.Second)
		if err != nil {
			return
		}
		defer server.Close()
		stopClose := context.AfterFunc(ctx, func() { client.Close(); server.Close() })
		defer stopClose()
		var matches atomic.Bool
		forwarded := make(chan struct{})
		go func() {
			defer close(forwarded)
			defer server.Close()
			var size [4]byte
			if _, err := io.ReadFull(client, size[:]); err != nil {
				return
			}
			n := binary.BigEndian.Uint32(size[:])
			if n < 4 || n > 1<<20 {
				return
			}
			startup := make([]byte, n)
			copy(startup, size[:])
			if _, err := io.ReadFull(client, startup[4:]); err != nil {
				return
			}
			if _, err := server.Write(startup); err != nil {
				return
			}
			statements := map[string]bool{}
			for {
				frame, err := pgFrame(client)
				if err != nil {
					return
				}
				switch frame[0] {
				case 'Q':
					matches.Store(bytes.Contains(frame[5:], []byte(statement)))
				case 'P':
					parts := bytes.SplitN(frame[5:], []byte{0}, 3)
					if len(parts) != 3 {
						return
					}
					statements[string(parts[0])] = bytes.Contains(parts[1], []byte(statement))
				case 'B':
					parts := bytes.SplitN(frame[5:], []byte{0}, 3)
					if len(parts) != 3 {
						return
					}
					matches.Store(statements[string(parts[1])])
				}
				if _, err = server.Write(frame); err != nil {
					return
				}
			}
		}()
		defer func() { client.Close(); server.Close(); <-forwarded }()
		holding := false
		for {
			frame, err := pgFrame(server)
			if err != nil {
				return
			}
			if frame[0] == 'C' && matches.Load() && armed.CompareAndSwap(true, false) {
				holding = true
			}
			if holding {
				if frame[0] == 'Z' {
					if len(frame) == 6 && frame[5] == 'I' {
						close(fired)
					}
					return
				}
				continue
			}
			if _, err = client.Write(frame); err != nil {
				return
			}
		}
	}()
	return u.String(), armed, fired
}

func pgFrame(reader io.Reader) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size < 4 || size > 16<<20 {
		return nil, io.ErrUnexpectedEOF
	}
	frame := make([]byte, size+1)
	copy(frame, header[:])
	_, err := io.ReadFull(reader, frame[5:])
	return frame, err
}

func TestSuccessfulSourceAckWithLostResponse(t *testing.T) {
	for _, deliveryMode := range []string{"fenced", "managed"} {
		const mode = "pgque"
		for _, wire := range []bool{false, true} {
			name := mode
			if wire {
				name = "outbox-" + mode
			}
			t.Run(deliveryMode+"/"+name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				e, err := lab.New(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				cfg := e.Config()
				cfg.DeliveryMode = deliveryMode
				if wire {
					cfg.OutboxStream = "events"
				}
				proxy, armed, fired := loseAckResponse(t, e.URL, "commit")
				cfg.DatabaseURL = proxy
				r, err := relay.Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				for {
					_, advanced, err := r.Step(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !advanced {
						break
					}
				}
				if wire {
					err = lab.WriteOutbox(ctx, e.DB, []int64{1}, []int64{1}, []string{"committed"})
				} else {
					err = lab.Write(ctx, e.DB, []int64{1}, []int64{1}, []string{"committed"})
				}
				if err != nil {
					t.Fatal(err)
				}
				if mode == "pgque" {
					_, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick(queue_name),pgque.ticker(queue_name) FROM pgque.queue")
					if err != nil {
						t.Fatal(err)
					}
				}
				armed.Store(true)
				if _, _, err = r.Step(ctx); err == nil {
					t.Fatal("lost ACK response reported success")
				}
				select {
				case <-fired:
				case <-ctx.Done():
					t.Fatal("committed ACK response was not intercepted")
				}
				r.Close()
				var remaining int
				query := "SELECT count(*) FROM pgque.subscription WHERE sub_batch IS NOT NULL"
				if err = e.DB.QueryRow(ctx, query).Scan(&remaining); err != nil || remaining != 0 {
					t.Fatal("ACK did not actually commit", remaining, err)
				}
				cfg.DatabaseURL = e.URL
				r, err = relay.Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				if n, _, err := r.Step(ctx); err != nil || n != 0 {
					t.Fatal("committed ACK was lost across restart", n, err)
				}
				consumer, err := e.Consumer()
				if err != nil {
					t.Fatal(err)
				}
				defer consumer.Close()
				if got := consumer.PollRecords(ctx, 1); got.NumRecords() != 1 {
					t.Fatal("acknowledged event missing", got.Errors())
				}
				poll, stop := context.WithTimeout(ctx, 100*time.Millisecond)
				defer stop()
				if consumer.PollRecords(poll, 1).NumRecords() != 0 {
					t.Fatal("successful source ACK unexpectedly replayed")
				}
			})
		}
	}
}
