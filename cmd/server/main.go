package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Priyanshu-1729/mini-etcd/server"
	"github.com/Priyanshu-1729/mini-etcd/store"
)

func main() {
	s := store.New()
	httpServer := server.NewHTTPServer(s)

	mux := http.NewServeMux()
	httpServer.RegisterRoutes(mux)

	addr := ":2379"
	fmt.Printf("mini-etcd listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}