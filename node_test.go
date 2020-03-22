package raft

import (
	"context"
	"testing"
	"time"
)

func TestBlockingSend(t *testing.T) {
	t.Parallel()

	ctx, _ := context.WithTimeout(context.Background(), time.Microsecond*3)
	n := newNode(42)

	if err := n.Send(ctx, nil); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded error, got %T", err)
	}
}

func TestShutdownSend(t *testing.T) {
	t.Parallel()

	n := Bootstrap(42)
	n.Cancel()

	ctx := context.Background()
	if err := n.Send(ctx, nil); err != ErrNodeShutdown {
		t.Fatalf("Send: expected ErrNodeShutdown error, got %T", err)
	}
}

func TestMultipleCancel(t *testing.T) {
	t.Parallel()

	n := Bootstrap(42)

	for i := 0; i < 10; i++ {
		n.Cancel()
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()

	n := Bootstrap(42)
	n.Cancel()

	select {
	case <-n.Done():
	default:
		t.Fatalf("done channel is not closed")
	}
}