package raft

import (
	"encoding/json"
	"log"
	"net/http"
)

type RaftServer struct {
	node *RaftNode
}

func NewRaftServer(node *RaftNode) *RaftServer {
	return &RaftServer{node: node}
}

func (rs *RaftServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/raft/vote", rs.handleVote)
	mux.HandleFunc("/raft/append", rs.handleAppend)
}

func (rs *RaftServer) handleVote(w http.ResponseWriter, r *http.Request) {
	var args RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reply := rs.node.HandleRequestVote(args)
	log.Printf("[%s] RequestVote from %s term=%d granted=%v",
		rs.node.id, args.CandidateID, args.Term, reply.VoteGranted)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (rs *RaftServer) handleAppend(w http.ResponseWriter, r *http.Request) {
	var args AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reply := rs.node.HandleAppendEntries(args)
	log.Printf("[%s] AppendEntries from %s term=%d success=%v entries=%d",
		rs.node.id, args.LeaderID, args.Term, reply.Success, len(args.Entries))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}
