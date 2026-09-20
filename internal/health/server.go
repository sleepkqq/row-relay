package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// Serve drains the probe server on cancellation and returns no driver/address
// details on failure. The listener is created synchronously by the caller.
func (h *Health) Serve(ctx context.Context, listener net.Listener) error {
	server := &http.Server{Handler: h.Handler(), ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ended, shutdown := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(shutdown)
		select {
		case <-ctx.Done():
			h.Stop()
			deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(deadline); err != nil {
				_ = server.Close()
			}
		case <-ended:
		}
	}()
	err := server.Serve(listener)
	close(ended)
	<-shutdown
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	h.Stop()
	return errors.New("health listener failed")
}
