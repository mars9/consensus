package raft

import "testing"

func TestNodeInitialization(t *testing.T) {
	n := newRaftNode(0x1, []NodeID{0x2, 0x3, 0x4, 0x5}...)
	if n.state != FollowerState {
		t.Errorf("init: expected state %q, got %q", FollowerState, n.state)
	}
	if n.leader != None {
		t.Errorf("init: expected none leader, got %d", n.leader)
	}
	if n.votedFor != None {
		t.Errorf("init: expected none votedFor, got %d", n.votedFor)
	}
	if n.id != 0x1 {
		t.Errorf("init: expected node id %d, got %d", 0x1, n.id)
	}
	if len(n.peers) != 4 {
		t.Errorf("init: expected zero peers, got %d", len(n.peers))
	}
}

func TestFollowerTick(t *testing.T) {
	n := newRaftNode(0x1, []NodeID{0x2, 0x3, 0x4, 0x5}...)
	n.Tick()

	if n.state != CandidateState {
		t.Errorf("follower: expected state %q, got %q", CandidateState, n.state)
	}
	if n.currentTerm != 1 {
		t.Errorf("follower: exepcted term 1, got %d", n.currentTerm)
	}
	if n.leader != None {
		t.Errorf("follower: expected none leader, got %d", n.leader)
	}
	if len(n.votes) != 1 {
		t.Errorf("follower: not voted")
	}
	if vote, _ := n.votes[0x1]; !vote {
		t.Errorf("follower: not voted")
	}
	if len(n.mbox) != 4 {
		t.Errorf("follower: expected 4 votes, got %d", len(n.mbox))
	}
}

func TestCandidateTick(t *testing.T) {
	n := newRaftNode(0x1, []NodeID{0x2, 0x3, 0x4, 0x5}...)
	n.Tick()
	n.Tick()

	if n.state != CandidateState {
		t.Errorf("candidate: expected state %q, got %q", CandidateState, n.state)
	}
	if n.currentTerm != 2 {
		t.Errorf("candidate: exepcted term 2, got %d", n.currentTerm)
	}
	if n.leader != None {
		t.Errorf("candidate: expected none leader, got %d", n.leader)
	}
	if len(n.votes) != 1 {
		t.Errorf("candidate: not voted")
	}
	if vote, _ := n.votes[0x1]; !vote {
		t.Errorf("candidate: not voted")
	}
	if len(n.mbox) != 8 {
		t.Errorf("candidate: expected 4 votes, got %d", len(n.mbox))
	}
}

func TestLeaderTick(t *testing.T) {
	n := newRaftNode(0x1, []NodeID{0x2, 0x3, 0x4, 0x5}...)
	n.currentTerm = 1
	n.leader = 0x1
	n.state = LeaderState
	n.Tick()

	if n.state != LeaderState {
		t.Errorf("leader: expected state %q, got %q", LeaderState, n.state)
	}
	if n.currentTerm != 1 {
		t.Errorf("leader: exepcted term 1, got %d", n.currentTerm)
	}
	if n.leader != 0x1 {
		t.Errorf("leader: expected 0x1 leader, got %d", n.leader)
	}
	if len(n.votes) != 0 {
		t.Errorf("leader: voted")
	}
	if len(n.mbox) != 4 {
		t.Errorf("leader: expected 0 heartbeats, got %d", len(n.mbox))
	}
}
