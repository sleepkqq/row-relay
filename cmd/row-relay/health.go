package main

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/sleepkqq/row-relay/internal/health"
)

func startProbes(ctx context.Context, stopRuntime context.CancelFunc, address string, state *health.Health) (func() error, error) {
	if address == "" {
		return func() error { state.Stop(); return nil }, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.New("cannot bind health listener")
	}
	serveCtx, stopServer := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		err := state.Serve(serveCtx, listener)
		if err != nil {
			stopRuntime()
		}
		done <- err
	}()
	return sync.OnceValue(func() error { state.Stop(); stopServer(); return <-done }), nil
}
