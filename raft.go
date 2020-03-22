// Package raft implements a consensus algorithm for managing a replicated log.
// Reference:
//		https://raft.github.io/raft.pdf
//
package raft

import "github.com/mars9/consensus/pb"

//go:generate stringer -type StateType -output stringer.go

// None is a placeholder cluster node ID used when there is no leader.
const None NodeID = 0

type NodeID uint64

type StateType uint8

const (
	FollowerState StateType = iota + 1
	CandidateState
	LeaderState
)

type peer struct {
	// for each server, index of the next log entry to send to that
	// server (initialized to leader last log index + 1)
	nextIndex uint64

	// for each server, index of highest log entry known to be
	// replicated on server (initialized to 0, increases monotonically)
	matchIndex uint64
}

type peers map[NodeID]*peer

func (p peers) quorum() int { return len(p)/2 + 1 }

// node implements a raft node and can act as a follower, candidate or a
// leader. A node is not safe for concurrent use.
type node struct {
	// §5: latest term server has seen (initialized to 0 on first boot,
	// increases monotonically)
	currentTerm uint64 // stable storage

	// §5: candidate that received vote in current term
	votedFor NodeID // stable storage

	// §5: index of highest log entry known to be committed(initialized
	// to 0, increases monotonically)
	commitIndex uint64

	// §5: index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically
	lastApplied uint64

	votes  map[NodeID]bool
	leader NodeID
	peers  peers

	// §5.2: When servers start up, they begin as followers
	state StateType

	// id is the identity of the local node
	id NodeID

	mbox []pb.Message
}

func newRaftNode(id NodeID, peers ...NodeID) *node {
	p := make(map[NodeID]*peer)
	for _, id := range peers {
		p[id] = &peer{}
	}
	return &node{
		state:       FollowerState,
		currentTerm: 0,
		votedFor:    None,
		commitIndex: 0,
		lastApplied: 0,
		leader:      None,
		peers:       p,
		id:          id,
	}
}

func (n *node) becomeFollower(term uint64, leader NodeID) {
	n.currentTerm = term
	n.votedFor = None
	n.votes = make(map[NodeID]bool)
	n.leader = leader
	n.state = FollowerState
}

func (n *node) becomeCandidate() {
	n.currentTerm++
	n.votedFor = n.id
	n.votes = make(map[NodeID]bool)
	n.votes[n.id] = true
	n.leader = None
	n.state = CandidateState
}

func (n *node) becomeLeader() {
	n.votedFor = None
	n.votes = make(map[NodeID]bool)
	n.leader = n.id
	n.state = LeaderState
}

func (n *node) campain() {
	for id := range n.peers {
		n.mbox = append(n.mbox, &pb.VoteRequest{
			From: uint64(n.id),
			To:   uint64(id),
			Term: n.currentTerm,
		})
	}
}

func (n *node) heartbeat() {
	for id := range n.peers {
		n.mbox = append(n.mbox, &pb.VoteResponse{
			From: uint64(n.id),
			To:   uint64(id),
			Term: n.currentTerm,
		})
	}
}

func (n *node) send(m pb.Message) {
	n.mbox = append(n.mbox, m)
}

func (n *node) Messages() []pb.Message {
	if len(n.mbox) == 0 {
		return nil
	}
	msgs := n.mbox[:len(n.mbox)-1]
	n.mbox = n.mbox[:0]
	return msgs
}

func (n *node) Tick() {
	switch n.state {
	case FollowerState, CandidateState:
		n.becomeCandidate()
		// §5.2: A candidate wins an election if it receives votes from
		// a majority of the servers in the full cluster for the same
		// term.
		n.campain()

	case LeaderState:
		n.heartbeat()
	}
}

func (n *node) Step(m pb.Message) {
	switch {
	// §5.1: If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	case m.GetTerm() > n.currentTerm:
		leader := NodeID(m.GetFrom())
		if _, ok := m.(*pb.VoteRequest); ok {
			leader = None
		}
		n.becomeFollower(m.GetTerm(), leader)

	case m.GetTerm() < n.currentTerm:
		return // ignore request from older term
	}

	switch m := m.(type) {
	case *pb.HeartbeatRequest:
		n.handleHeartbeatRequest(m)
	case *pb.HeartbeatResponse:
		n.handleHeartbeatResponse(m)
	case *pb.VoteRequest:
		n.handleVoteRequest(m)
	case *pb.VoteResponse:
		n.handleVoteResponse(m)
	}
}

func (n *node) handleVoteRequest(req *pb.VoteRequest) {
	resp := &pb.VoteResponse{
		From:    uint64(n.id),
		To:      uint64(req.From),
		Term:    n.currentTerm,
		Success: false,
	}
	from := NodeID(req.From)

	switch n.state {
	case FollowerState:
		if n.votedFor == None || n.votedFor == from {
			n.votedFor = from
			resp.Success = true
		}
	case CandidateState, LeaderState:
		// nothing
	}

	n.send(resp)
}

func (n *node) handleVoteResponse(resp *pb.VoteResponse) {
	switch n.state {
	case FollowerState:
		// nothing
	case CandidateState:
		n.votes[NodeID(resp.From)] = resp.Success
		var granted int
		for _, success := range n.votes {
			if success {
				granted++
			}
		}
		switch n.peers.quorum() {
		case granted:
			n.becomeLeader()
			n.heartbeat()
		case len(n.votes) - granted:
			n.becomeFollower(n.currentTerm, None)
		}
	case LeaderState:
		// nothing
	}
}

func (n *node) handleHeartbeatRequest(req *pb.HeartbeatRequest) {}

func (n *node) handleHeartbeatResponse(resp *pb.HeartbeatResponse) {}
