package api

import (
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/TomStuart92/asfalis/pkg/raft/raftpb"
	"github.com/TomStuart92/asfalis/pkg/store"
)

type httpAPI struct {
	store       *store.DistributedStore
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	switch {
	case r.Method == "PUT":
		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed to PUT", http.StatusBadRequest)
			return
		}
		h.store.Propose(key, string(value))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if value, ok := h.store.Lookup(key); ok {
			w.Write([]byte(value))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "DELETE":
		h.store.Propose(key, "")
		w.WriteHeader(http.StatusNoContent)
		return
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHTTP starts HTTP server
func ServeHTTP(kv *store.DistributedStore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpAPI{
			store:       kv,
			confChangeC: confChangeC,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
