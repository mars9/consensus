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
	if err := n.Send(ctx, nil); err != errNodeShutdown {
		t.Fatalf("Send: expected errNodeShutdown error, got %T", err)
	}
	if err := n.send(nil); err != errNodeShutdown {
		t.Fatalf("send: expected errNodeShutdown error, got %T", err)
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

func TestBootstrap(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	N := 10
	var res []*Message
	n := Bootstrap(42)

	go func() {
		defer close(done)
		for {
			select {
			case m := <-n.Recv():
				res = append(res, m)
			case <-n.Done():
				return
			}
		}
	}()

	for i := 0; i < N; i++ {
		n.send(&Message{Type: MessageType(i)})
	}
	n.Cancel()
	<-done
	if len(res) != N {
		t.Fatalf("expected %d messages, got %d", N, len(res))
	}
}
