package api

import (
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/TomStuart92/asfalis/pkg/logger"
	"github.com/TomStuart92/asfalis/pkg/store"
)

var log *logger.Logger = logger.NewStdoutLogger("HTTP-API: ")

type httpAPI struct {
	store       *store.DistributedStore
}

func (h *httpAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	switch {
	case r.Method == "PUT":
		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed to PUT", http.StatusBadRequest)
			return
		}
		h.store.Propose(key, string(value))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if value, ok := h.store.Lookup(key); ok {
			_, err := w.Write([]byte(value))
			if err != nil {
				log.Errorf("Write failed: %v", err)
			}
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
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHTTP starts HTTP server
func ServeHTTP(kv *store.DistributedStore, port int, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpAPI{
			store:       kv,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if _, ok := <-errorC; ok {
		srv.Close()
	}
}
