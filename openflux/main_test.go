package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeTransportState struct{ running, connected atomic.Bool }

func (s *fakeTransportState) IsRunning() bool   { return s.running.Load() }
func (s *fakeTransportState) IsConnected() bool { return s.connected.Load() }

func TestAuthenticationFailsOnStoppedWorker(t *testing.T) {
	state := &fakeTransportState{}
	start := time.Now()
	if err := waitAuthenticated(context.Background(), state, time.Second, time.Millisecond); err == nil {
		t.Fatal("stopped worker reported ready")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("waited full authentication timeout after terminal failure")
	}
}

func TestMonitorAllowsReconnectButReportsTerminalFailure(t *testing.T) {
	state := &fakeTransportState{}
	state.running.Store(true)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- monitorTransport(ctx, state, nil, nil, time.Millisecond) }()
	// A transport that is disconnected but still running is reconnecting.
	select {
	case err := <-done:
		t.Fatalf("reconnection treated as terminal: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	state.running.Store(false)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("terminal failure returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("monitor ignored terminal failure")
	}
}

func TestMonitorPropagatesListenerFailure(t *testing.T) {
	state := &fakeTransportState{}
	state.running.Store(true)
	want := errors.New("listen address already in use")
	result := make(chan error, 1)
	result <- want
	if got := monitorTransport(context.Background(), state, result, nil, time.Millisecond); !errors.Is(got, want) {
		t.Fatalf("got %v, want listener failure", got)
	}
}

func TestMonitorCancellationIsSuccessfulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := monitorTransport(ctx, &fakeTransportState{}, nil, nil, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := waitAuthenticated(ctx, &fakeTransportState{}, time.Second, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
