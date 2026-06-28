package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/Priyanshu-1729/mini-etcd/raft"
	"github.com/Priyanshu-1729/mini-etcd/server"
	"github.com/Priyanshu-1729/mini-etcd/store"
)

func main() {
	id   := flag.String("id",   "node1",            "node ID")
	addr := flag.String("addr", "localhost:2379",    "listen address")
	flag.Parse()

	// all nodes in the cluster and their Raft RPC addresses
	allNodes := map[string]string{
		"node1": "http://localhost:2379",
		"node2": "http://localhost:2380",
		"node3": "http://localhost:2381",
	}

	// peers = everyone except myself
	peers := []string{}
	peerURLs := map[string]string{}
	for nodeID, url := range allNodes {
		if nodeID != *id {
			peers = append(peers, nodeID)
			peerURLs[nodeID] = url
		}
	}

	transport := raft.NewHTTPTransport(peerURLs)
	raftNode  := raft.NewRaftNode(*id, peers, transport)
	raftServer := raft.NewRaftServer(raftNode)

	s := store.New()
	httpServer := server.NewHTTPServer(s)

	mux := http.NewServeMux()
	httpServer.RegisterRoutes(mux)   // /v1/kv/*
	raftServer.RegisterRoutes(mux)   // /raft/vote  /raft/append

	fmt.Printf("[%s] listening on %s\n", *id, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}