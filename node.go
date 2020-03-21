package raft

import (
	"context"
	"errors"
	"sync"
)

// Node implements a raft node and can act as a follower, candidate or a
// leader.
type Node struct {
	// id is the identity of the local node; id cannot be 0
	id NodeID

	input  chan *Message
	output chan *Message

	onceCanceler sync.Once
	done         chan struct{}
	stopped      chan struct{}

	// latest term server has seen (initialized to 0 on first boot,
	// increases monotonically)
	term uint64 // stable storage

	// candidate that received vote in current terms
	voted NodeID // stable storage

	//commit uint16

	votes  map[NodeID]struct{}
	leader NodeID
	state  stateType
	peers  map[NodeID]*peer
}

func newNode(id NodeID) *Node {
	return &Node{
		id:      id,
		input:   make(chan *Message),
		output:  make(chan *Message),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),

		term:   0,
		voted:  none,
		leader: none,
	}
}

func Bootstrap(id NodeID) *Node {
	n := newNode(id)
	n.becomeFollower(0, none)
	go n.run()
	return n
}

var errNodeShutdown = errors.New("node is shut down")

// Entry represents a replicated log entry.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type MessageType uint8

const (
	RequestVoteRequest MessageType = iota + 100
	RequestVoteResponse
	AppendEntriesRequest
	AppendEntriesReponse
)

// Message represents a raft RPC message.
type Message struct {
	Type     MessageType
	From     NodeID
	To       NodeID
	Term     uint64
	LogIndex uint64
	LogTerm  uint64
	Entries  []*Entry
	Commit   uint64
	//Success  bool
}

type NodeID uint32

type peer struct {
	// index of the next log entry to send to that server (initialized
	// to leader last log index + 1)
	next uint64

	// index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	match uint64
}

const (
	stateFollower  stateType = 0x1
	stateCandidate stateType = 0x2
	stateLeader    stateType = 0x3

	none = 0x0
)

// StateType represents the role of a node in a cluster.
type stateType uint8

func (s stateType) String() string {
	switch s {
	case stateFollower:
		return "follower"
	case stateCandidate:
		return "candidate"
	case stateLeader:
		return "leader"
	}
	return "unknown"
}

func (n *Node) Send(ctx context.Context, m *Message) error {
	select {
	case n.input <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return errNodeShutdown
	}
}

func (n *Node) Recv() <-chan *Message { return n.output }

func (n *Node) send(m *Message) error {
	select {
	case n.output <- m:
		return nil
	case <-n.done:
		return errNodeShutdown
	}
}

func (n *Node) Done() <-chan struct{} { return n.done }

func (n *Node) Cancel() {
	n.onceCanceler.Do(func() {
		close(n.done)
		<-n.stopped
	})
}

func (n *Node) run() {
	for {
		select {
		case <-n.input:
		case <-n.done:
			close(n.stopped)
			return
		}
	}
}

func (n *Node) campain() {
	for id := range n.peers {
		n.send(&Message{
			Type: RequestVoteRequest,
			From: n.id,
			To:   id,
			Term: n.term,
		})
	}
}

func (n *Node) becomeFollower(term uint64, leader NodeID) {
	n.term = term
	n.voted = none
	n.votes = make(map[NodeID]struct{})
	n.leader = leader
	n.state = stateFollower
}

func (n *Node) becomeCandidate() {
	n.term = n.term + 1
	n.voted = n.id
	n.votes = make(map[NodeID]struct{})
	n.votes[n.id] = struct{}{}
	n.leader = none
	n.state = stateCandidate
}

func (n *Node) becomeLeader() {
	n.term = n.term
	n.voted = none
	n.votes = make(map[NodeID]struct{})
	n.leader = n.id
	n.state = stateLeader
}
