package relay

import (
	"context"
	"testing"
)

// A rejected stream set must not run either callback: activity is tied to real
// operations, never to configuration validation.
func TestRunStreamsActivityRejectsInvalidCountWithoutCallbacks(t *testing.T) {
	reported, touched := false, false
	err := RunStreamsActivity(context.Background(), nil,
		func(string, error) { reported = true },
		func(string) { touched = true })
	if err == nil || reported || touched {
		t.Fatalf("invalid stream count ran callbacks: err=%v reported=%v touched=%v", err, reported, touched)
	}
}
