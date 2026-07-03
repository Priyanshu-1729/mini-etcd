package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/Priyanshu-1729/mini-etcd/api"
	pb "github.com/Priyanshu-1729/mini-etcd/proto"
	"github.com/Priyanshu-1729/mini-etcd/raft"
	"github.com/Priyanshu-1729/mini-etcd/server"
	"github.com/Priyanshu-1729/mini-etcd/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	id       := flag.String("id",        "node1",           "node ID")
	httpAddr := flag.String("addr",      "localhost:2379",  "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "localhost:50051", "gRPC listen address")
	flag.Parse()

	allNodes := map[string]string{
		"node1": "http://localhost:2379",
		"node2": "http://localhost:2380",
		"node3": "http://localhost:2381",
	}

	peers    := []string{}
	peerURLs := map[string]string{}
	for nodeID, url := range allNodes {
		if nodeID != *id {
			peers = append(peers, nodeID)
			peerURLs[nodeID] = url
		}
	}

	s         := store.New()
	transport := raft.NewHTTPTransport(peerURLs)
	raftNode  := raft.NewRaftNode(*id, peers, transport)
	raftSrv   := raft.NewRaftServer(raftNode)

	// apply committed Raft entries to the local store
	go func() {
		for msg := range raftNode.ApplyCh() {
			var cmd api.Command
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				log.Printf("[%s] bad command at index %d: %v", *id, msg.Index, err)
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

	// --- HTTP server (REST + Raft RPC) ---
	httpSrv := server.NewHTTPServer(s, raftNode)
	mux     := http.NewServeMux()
	httpSrv.RegisterRoutes(mux)
	raftSrv.RegisterRoutes(mux)

	go func() {
		fmt.Printf("[%s] HTTP listening on %s\n", *id, *httpAddr)
		log.Fatal(http.ListenAndServe(*httpAddr, mux))
	}()

	// --- gRPC server ---
	// reflection lets grpcurl and other tools discover services at runtime
	grpcSrv := grpc.NewServer()
	pb.RegisterKVServiceServer(grpcSrv, server.NewGRPCServer(s, raftNode))
	reflection.Register(grpcSrv)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *grpcAddr, err)
	}
	fmt.Printf("[%s] gRPC listening on %s\n", *id, *grpcAddr)
	log.Fatal(grpcSrv.Serve(lis))
}
