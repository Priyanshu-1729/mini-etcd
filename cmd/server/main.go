package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Priyanshu-1729/mini-etcd/api"
	pb "github.com/Priyanshu-1729/mini-etcd/proto"
	"github.com/Priyanshu-1729/mini-etcd/raft"
	"github.com/Priyanshu-1729/mini-etcd/server"
	"github.com/Priyanshu-1729/mini-etcd/snapshot"
	"github.com/Priyanshu-1729/mini-etcd/store"
	w "github.com/Priyanshu-1729/mini-etcd/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	id       := flag.String("id",        "node1",           "node ID")
	httpAddr := flag.String("addr",      "localhost:2379",  "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "localhost:50051", "gRPC listen address")
	dataDir  := flag.String("data-dir",  "data",            "directory for WAL and snapshots")
	flag.Parse()

	nodeDir  := filepath.Join(*dataDir, *id)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}
	walPath  := filepath.Join(nodeDir, "raft.wal")
	snapPath := filepath.Join(nodeDir, "snapshot.json")

	s := store.New()
	snap, err := snapshot.Load(snapPath)
	if err != nil {
		log.Fatalf("failed to load snapshot: %v", err)
	}
	if snap != nil {
		log.Printf("[%s] restoring snapshot at index=%d", *id, snap.LastIndex)
		for k, v := range snap.Data {
			s.Put(k, v)
		}
	}

	wal, err := w.Open(walPath)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	records, err := wal.ReadAll()
	if err != nil {
		log.Fatalf("failed to replay WAL: %v", err)
	}
	startIndex := uint64(0)
	if snap != nil {
		startIndex = snap.LastIndex
	}
	for _, rec := range records {
		if rec.Index <= startIndex {
			continue
		}
		var cmd api.Command
		if err := json.Unmarshal(rec.Command, &cmd); err != nil {
			continue
		}
		switch cmd.Op {
		case "put":
			s.Put(cmd.Key, cmd.Value)
		case "delete":
			s.Delete(cmd.Key)
		}
	}
	log.Printf("[%s] replayed %d WAL records", *id, len(records))

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

	transport := raft.NewHTTPTransport(peerURLs)
	raftNode  := raft.NewRaftNode(*id, peers, transport)
	raftSrv   := raft.NewRaftServer(raftNode)

	applyCount := startIndex
	go func() {
		for msg := range raftNode.ApplyCh() {
			var cmd api.Command
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				log.Printf("[%s] bad command at index %d: %v", *id, msg.Index, err)
				continue
			}
			if err := wal.Append(msg.Index, msg.Index, msg.Command); err != nil {
				log.Printf("[%s] WAL append failed: %v", *id, err)
			}
			switch cmd.Op {
			case "put":
				s.Put(cmd.Key, cmd.Value)
				log.Printf("[%s] applied put key=%s value=%s index=%d", *id, cmd.Key, cmd.Value, msg.Index)
			case "delete":
				s.Delete(cmd.Key)
				log.Printf("[%s] applied delete key=%s index=%d", *id, cmd.Key, msg.Index)
			}
			applyCount++
			if applyCount%10 == 0 {
				data := s.Snapshot()
				snap := snapshot.Snapshot{
					LastIndex: msg.Index,
					LastTerm:  msg.Index,
					Data:      data,
				}
				if err := snapshot.Save(snapPath, snap); err != nil {
					log.Printf("[%s] snapshot failed: %v", *id, err)
				} else {
					log.Printf("[%s] snapshot saved at index=%d", *id, msg.Index)
				}
			}
		}
	}()

	// pass raftNode as both Proposer and StatusReporter
	httpSrv := server.NewHTTPServer(s, raftNode, raftNode)
	mux     := http.NewServeMux()
	httpSrv.RegisterRoutes(mux)
	raftSrv.RegisterRoutes(mux)

	go func() {
		fmt.Printf("[%s] HTTP listening on %s\n", *id, *httpAddr)
		log.Fatal(http.ListenAndServe(*httpAddr, mux))
	}()

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
