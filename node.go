package raft

import (
	"context"
	"errors"
	"sync"

	"github.com/mars9/consensus/pb"
)

// Node implements a raft node and can act as a follower, candidate or a
// leader.
type Node struct {
	outputc chan []pb.Message
	inputc  chan pb.Message

	tickc    chan struct{}
	canceler sync.Once
	donec    chan struct{}
	stopc    chan struct{}

	raft *node
}

func newNode(id NodeID, peers ...NodeID) *Node {
	return &Node{
		outputc: make(chan []pb.Message),
		inputc:  make(chan pb.Message),
		tickc:   make(chan struct{}),
		donec:   make(chan struct{}),
		stopc:   make(chan struct{}),
		raft:    newRaftNode(id, peers...),
	}
}

func Bootstrap(id NodeID, peers ...NodeID) *Node {
	n := newNode(id, peers...)
	go n.run()
	return n
}

func (n *Node) State() StateType { return n.raft.state }

var ErrNodeShutdown = errors.New("node is shut down")

func (n *Node) Done() <-chan struct{} { return n.donec }

func (n *Node) Cancel() {
	n.canceler.Do(func() {
		close(n.donec)
		<-n.stopc
	})
}

func (n *Node) Send(ctx context.Context, m pb.Message) error {
	select {
	case n.inputc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrNodeShutdown
	}
}

func (n *Node) Tick() error {
	select {
	case n.tickc <- struct{}{}:
		return nil
	case <-n.donec:
		return ErrNodeShutdown
	}
}

func (n *Node) Recv() <-chan []pb.Message { return n.outputc }

func (n *Node) send() {
	msgs := n.raft.Messages()
	if msgs == nil {
		return
	}
	n.outputc <- msgs
}

func (n *Node) run() {
	for {
		select {
		case <-n.inputc:
			// TODO
		case <-n.tickc:
			n.raft.Tick()
			n.send()
		case <-n.donec:
			close(n.stopc)
			// Safe to close here. We're the only sender and closer
			close(n.outputc)
			return
		}
	}
}
