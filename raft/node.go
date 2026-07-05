package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type NodeState int

const (
	Follower  NodeState = iota
	Candidate NodeState = iota
	Leader    NodeState = iota
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

type Transport interface {
	RequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

type ApplyMsg struct {
	Index   uint64
	Command []byte
}

type RaftNode struct {
	mu          sync.Mutex
	id          string
	peers       []string
	transport   Transport
	currentTerm uint64
	votedFor    string
	log         *Log
	commitIndex uint64
	lastApplied uint64
	state       NodeState
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	applyCh     chan ApplyMsg
	heartbeatCh chan struct{}
	grantVoteCh chan struct{}
	stepDownCh  chan struct{}
	stopCh      chan struct{}
}

func NewRaftNode(id string, peers []string, transport Transport) *RaftNode {
	rn := &RaftNode{
		id:          id,
		peers:       peers,
		transport:   transport,
		log:         NewLog(),
		state:       Follower,
		applyCh:     make(chan ApplyMsg, 100),
		heartbeatCh: make(chan struct{}, 1),
		grantVoteCh: make(chan struct{}, 1),
		stepDownCh:  make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
	}
	go rn.run()
	go rn.applyLoop()
	return rn
}

func (rn *RaftNode) Stop() { close(rn.stopCh) }

func (rn *RaftNode) ApplyCh() <-chan ApplyMsg { return rn.applyCh }

func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.stopCh:
			return
		default:
		}
		rn.mu.Lock()
		state := rn.state
		rn.mu.Unlock()
		switch state {
		case Follower:
			rn.runFollower()
		case Candidate:
			rn.runCandidate()
		case Leader:
			rn.runLeader()
		}
	}
}

func (rn *RaftNode) electionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (rn *RaftNode) runFollower() {
	timer := time.NewTimer(rn.electionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.heartbeatCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(rn.electionTimeout())
		case <-rn.grantVoteCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(rn.electionTimeout())
		case <-timer.C:
			log.Printf("[%s] election timeout, becoming candidate", rn.id)
			rn.mu.Lock()
			rn.state = Candidate
			rn.mu.Unlock()
			return
		}
	}
}

func (rn *RaftNode) runCandidate() {
	rn.mu.Lock()
	rn.currentTerm++
	rn.votedFor = rn.id
	term := rn.currentTerm
	lastIndex := rn.log.LastIndex()
	lastTerm := rn.log.LastTerm()
	rn.mu.Unlock()

	log.Printf("[%s] starting election for term %d", rn.id, term)

	votes := 1
	majority := (len(rn.peers)+1)/2 + 1
	voteCh := make(chan bool, len(rn.peers))

	for _, peer := range rn.peers {
		go func(peer string) {
			reply, err := rn.transport.RequestVote(peer, RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				voteCh <- false
				return
			}
			rn.mu.Lock()
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.state = Follower
				rn.votedFor = ""
				rn.mu.Unlock()
				voteCh <- false
				return
			}
			rn.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(peer)
	}

	timer := time.NewTimer(rn.electionTimeout())
	defer timer.Stop()

	for i := 0; i < len(rn.peers); i++ {
		select {
		case <-rn.stopCh:
			return
		case <-rn.stepDownCh:
			rn.mu.Lock()
			rn.state = Follower
			rn.mu.Unlock()
			return
		case <-timer.C:
			log.Printf("[%s] election timed out, retrying", rn.id)
			return
		case granted := <-voteCh:
			if granted {
				votes++
			}
			if votes >= majority {
				log.Printf("[%s] won election for term %d", rn.id, term)
				rn.mu.Lock()
				rn.state = Leader
				for _, p := range rn.peers {
					rn.nextIndex[p] = rn.log.LastIndex() + 1
					rn.matchIndex[p] = 0
				}
				rn.mu.Unlock()
				return
			}
		}
	}
}

func (rn *RaftNode) runLeader() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	rn.broadcastHeartbeat()
	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.stepDownCh:
			rn.mu.Lock()
			rn.state = Follower
			rn.mu.Unlock()
			return
		case <-ticker.C:
			rn.broadcastHeartbeat()
		}
	}
}

func (rn *RaftNode) broadcastHeartbeat() {
	rn.mu.Lock()
	term := rn.currentTerm
	leaderID := rn.id
	commitIndex := rn.commitIndex
	rn.mu.Unlock()

	for _, peer := range rn.peers {
		go func(peer string) {
			rn.mu.Lock()
			prevIndex := rn.nextIndex[peer] - 1
			prevEntry, err := rn.log.Entry(prevIndex)
			if err != nil {
				rn.mu.Unlock()
				return
			}
			entries := rn.log.Slice(rn.nextIndex[peer], rn.log.LastIndex()+1)
			rn.mu.Unlock()

			reply, err := rn.transport.AppendEntries(peer, AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevEntry.Term,
				Entries:      entries,
				LeaderCommit: commitIndex,
			})
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.state = Follower
				rn.votedFor = ""
				select {
				case rn.stepDownCh <- struct{}{}:
				default:
				}
				return
			}
			if reply.Success {
				rn.matchIndex[peer] = prevIndex + uint64(len(entries))
				rn.nextIndex[peer] = rn.matchIndex[peer] + 1
				rn.maybeAdvanceCommit()
			} else {
				if rn.nextIndex[peer] > 1 {
					rn.nextIndex[peer]--
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) maybeAdvanceCommit() {
	for n := rn.log.LastIndex(); n > rn.commitIndex; n-- {
		entry, err := rn.log.Entry(n)
		if err != nil || entry.Term != rn.currentTerm {
			continue
		}
		matches := 1
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= n {
				matches++
			}
		}
		if matches >= (len(rn.peers)+1)/2+1 {
			rn.commitIndex = n
			break
		}
	}
}

func (rn *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := RequestVoteReply{Term: rn.currentTerm}
	if args.Term < rn.currentTerm {
		return reply
	}
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = ""
	}
	logOK := args.LastLogTerm > rn.log.LastTerm() ||
		(args.LastLogTerm == rn.log.LastTerm() && args.LastLogIndex >= rn.log.LastIndex())
	if (rn.votedFor == "" || rn.votedFor == args.CandidateID) && logOK {
		rn.votedFor = args.CandidateID
		reply.VoteGranted = true
		select {
		case rn.grantVoteCh <- struct{}{}:
		default:
		}
	}
	reply.Term = rn.currentTerm
	return reply
}

func (rn *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := AppendEntriesReply{Term: rn.currentTerm}
	if args.Term < rn.currentTerm {
		return reply
	}
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = ""
	}
	rn.state = Follower
	select {
	case rn.heartbeatCh <- struct{}{}:
	default:
	}

	if args.PrevLogIndex > rn.log.LastIndex() {
		reply.ConflictIndex = rn.log.LastIndex() + 1
		return reply
	}
	prevEntry, err := rn.log.Entry(args.PrevLogIndex)
	if err != nil || prevEntry.Term != args.PrevLogTerm {
		reply.ConflictTerm = prevEntry.Term
		reply.ConflictIndex = args.PrevLogIndex
		return reply
	}
	rn.log.Append(args.PrevLogIndex, args.Entries)
	if args.LeaderCommit > rn.commitIndex {
		if args.LeaderCommit < rn.log.LastIndex() {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = rn.log.LastIndex()
		}
	}
	reply.Success = true
	reply.Term = rn.currentTerm
	return reply
}

func (rn *RaftNode) applyLoop() {
	for {
		select {
		case <-rn.stopCh:
			return
		case <-time.After(10 * time.Millisecond):
			rn.mu.Lock()
			for rn.lastApplied < rn.commitIndex {
				rn.lastApplied++
				entry, err := rn.log.Entry(rn.lastApplied)
				if err != nil {
					rn.mu.Unlock()
					continue
				}
				msg := ApplyMsg{Index: rn.lastApplied, Command: entry.Command}
				rn.mu.Unlock()
				rn.applyCh <- msg
				rn.mu.Lock()
			}
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) Propose(command []byte) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state != Leader {
		return false
	}
	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.log.LastIndex() + 1,
		Command: command,
	}
	rn.log.Append(rn.log.LastIndex(), []LogEntry{entry})
	return true
}

// Status returns a snapshot of this node's current Raft state.
// Used by the /status HTTP endpoint for observability.
func (rn *RaftNode) Status() NodeStatus {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	stateStr := "follower"
	if rn.state == Candidate {
		stateStr = "candidate"
	} else if rn.state == Leader {
		stateStr = "leader"
	}
	return NodeStatus{
		ID:       rn.id,
		State:    stateStr,
		Term:     rn.currentTerm,
		IsLeader: rn.state == Leader,
	}
}

type NodeStatus struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	IsLeader bool   `json:"is_leader"`
}
