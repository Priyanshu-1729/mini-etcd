package server

import (
	"context"
	"encoding/json"

	"github.com/Priyanshu-1729/mini-etcd/api"
	pb "github.com/Priyanshu-1729/mini-etcd/proto"
	"github.com/Priyanshu-1729/mini-etcd/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the KVService proto interface.
// Writes go through Raft consensus via the Proposer interface.
// Reads are served directly from the local state machine for now;
// linearizable reads via ReadIndex will be added in the next commit.
type GRPCServer struct {
	pb.UnimplementedKVServiceServer
	store    *store.Store
	proposer Proposer
}

func NewGRPCServer(s *store.Store, p Proposer) *GRPCServer {
	return &GRPCServer{store: s, proposer: p}
}

func (g *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	val, found := g.store.Get(req.Key)
	return &pb.GetResponse{Key: req.Key, Value: val, Found: found}, nil
}

func (g *GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	cmd := api.Command{Op: "put", Key: req.Key, Value: req.Value}
	data, _ := json.Marshal(cmd)
	if !g.proposer.Propose(data) {
		// this node is not the leader — client should retry on another node
		return nil, status.Error(codes.Unavailable, "not the leader")
	}
	return &pb.PutResponse{Success: true}, nil
}

func (g *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	cmd := api.Command{Op: "delete", Key: req.Key}
	data, _ := json.Marshal(cmd)
	if !g.proposer.Propose(data) {
		return nil, status.Error(codes.Unavailable, "not the leader")
	}
	return &pb.DeleteResponse{Success: true}, nil
}

// Watch streams WatchEvents to the client whenever the watched key changes.
// The stream stays open until the client disconnects or the context is cancelled.
func (g *GRPCServer) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	ch, cancel := g.store.Watch(req.Key)
	defer cancel()

	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.WatchEvent{
				Type:  string(event.Type),
				Key:   event.Key,
				Value: event.Value,
			}); err != nil {
				// client disconnected
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}
