package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// Only Detach matters here; the rest is never called.
type detachingBackend struct {
	backend
	refusals int
	calls    []string
}

func (b *detachingBackend) Detach(ctx context.Context, vol *cinderVolume, serverID string) error {
	b.calls = append(b.calls, serverID)
	if b.refusals > 0 {
		b.refusals--
		return fmt.Errorf("%w: server rebooting", errConflict)
	}
	return nil
}

func TestDetachIsRetriedWhileTheServerIsBusy(t *testing.T) {
	detachRetryInterval = time.Millisecond
	b := &detachingBackend{refusals: 2}
	p := plugin{backend: b}

	if err := p.detachWithRetry(context.Background(), &cinderVolume{Name: "data"}, "i-2"); err != nil {
		t.Fatal(err)
	}
	if len(b.calls) != 3 || b.calls[2] != "i-2" {
		t.Fatalf("calls %v", b.calls)
	}
}

func TestAnyOtherDetachErrorIsNotRetried(t *testing.T) {
	detachRetryInterval = time.Millisecond
	p := plugin{backend: &failingBackend{}}

	if err := p.detachWithRetry(context.Background(), &cinderVolume{Name: "data"}, "i-2"); err == nil || errors.Is(err, errConflict) {
		t.Fatalf("want the error back, got %v", err)
	}
}

type failingBackend struct{ backend }

func (b *failingBackend) Detach(ctx context.Context, vol *cinderVolume, serverID string) error {
	return errors.New("forbidden")
}
