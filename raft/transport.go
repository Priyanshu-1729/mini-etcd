package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type HTTPTransport struct {
	client *http.Client
	peers  map[string]string
}

func NewHTTPTransport(peers map[string]string) *HTTPTransport {
	return &HTTPTransport{
		peers: peers,
		client: &http.Client{Timeout: 100 * time.Millisecond},
	}
}

func (t *HTTPTransport) RequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.post(peerID, "/raft/vote", args, &reply)
	return reply, err
}

func (t *HTTPTransport) AppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.post(peerID, "/raft/append", args, &reply)
	return reply, err
}

func (t *HTTPTransport) post(peerID, path string, body, reply any) error {
	url, ok := t.peers[peerID]
	if !ok {
		return fmt.Errorf("unknown peer: %s", peerID)
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(url+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(reply)
}
