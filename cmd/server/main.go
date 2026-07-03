package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/Priyanshu-1729/mini-etcd/api"
	"github.com/Priyanshu-1729/mini-etcd/raft"
	"github.com/Priyanshu-1729/mini-etcd/server"
	"github.com/Priyanshu-1729/mini-etcd/store"
)

func main() {
	id   := flag.String("id",   "node1",         "node ID")
	addr := flag.String("addr", "localhost:2379", "listen address")
	flag.Parse()

	allNodes := map[string]string{
		"node1": "http://localhost:2379",
		"node2": "http://localhost:2380",
		"node3": "http://localhost:2381",
	}

	peers := []string{}
	peerURLs := map[string]string{}
	for nodeID, url := range allNodes {
		if nodeID != *id {
			peers = append(peers, nodeID)
			peerURLs[nodeID] = url
		}
	}

	s := store.New()
	transport := raft.NewHTTPTransport(peerURLs)
	raftNode  := raft.NewRaftNode(*id, peers, transport)
	raftServer := raft.NewRaftServer(raftNode)

	// apply loop: committed Raft entries → store
	go func() {
		for msg := range raftNode.ApplyCh() {
			var cmd api.Command
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				log.Printf("bad command at index %d: %v", msg.Index, err)
				continue
			}
			switch cmd.Op {
			case "put":
				s.Put(cmd.Key, cmd.Value)
				log.Printf("[%s] applied put key=%s value=%s index=%d", *id, cmd.Key, cmd.Value, msg.Index)
			case "delete":
				s.Delete(cmd.Key)
				log.Printf("[%s] applied delete key=%s index=%d", *id, cmd.Key, msg.Index)
			}
		}
	}()

	httpServer := server.NewHTTPServer(s, raftNode)

	mux := http.NewServeMux()
	httpServer.RegisterRoutes(mux)
	raftServer.RegisterRoutes(mux)

	fmt.Printf("[%s] listening on %s\n", *id, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
