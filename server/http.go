package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Priyanshu-1729/mini-etcd/api"
	"github.com/Priyanshu-1729/mini-etcd/raft"
	"github.com/Priyanshu-1729/mini-etcd/store"
)

type Proposer interface {
	Propose(command []byte) bool
}

type StatusReporter interface {
	Status() raft.NodeStatus
}

type HTTPServer struct {
	store    *store.Store
	proposer Proposer
	reporter StatusReporter
}

func NewHTTPServer(s *store.Store, p Proposer, r StatusReporter) *HTTPServer {
	return &HTTPServer{store: s, proposer: p, reporter: r}
}

func (h *HTTPServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/kv/get", h.handleGet)
	mux.HandleFunc("/v1/kv/put", h.handlePut)
	mux.HandleFunc("/v1/kv/delete", h.handleDelete)
	mux.HandleFunc("/v1/kv/watch", h.handleWatch)
	mux.HandleFunc("/status", h.handleStatus)
}

func (h *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, h.reporter.Status())
}

func (h *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	val, found := h.store.Get(key)
	writeJSON(w, api.GetResponse{Key: key, Value: val, Found: found})
}

func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req api.PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	cmd := api.Command{Op: "put", Key: req.Key, Value: req.Value}
	data, _ := json.Marshal(cmd)
	if !h.proposer.Propose(data) {
		http.Error(w, "not the leader", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, api.PutResponse{Success: true})
}

func (h *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	cmd := api.Command{Op: "delete", Key: key}
	data, _ := json.Marshal(cmd)
	if !h.proposer.Propose(data) {
		http.Error(w, "not the leader", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, api.DeleteResponse{Success: true})
}

func (h *HTTPServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	ch, cancel := h.store.Watch(key)
	defer cancel()

	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
