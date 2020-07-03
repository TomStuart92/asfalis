package api

import (
	"context"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/TomStuart92/asfalis/pkg/logger"
	"github.com/TomStuart92/asfalis/pkg/store"
)

var log *logger.Logger = logger.NewStdoutLogger("api")
var srv http.Server

type httpAPI struct {
	store *store.DistributedStore
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
		log.Infof("Received request to set key %s to value %s", key, value)
		h.store.Propose(key, string(value))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		log.Infof("Received request to get key %s", key)
		if value, ok := h.store.Lookup(key); ok {
			_, err := w.Write([]byte(value))
			if err != nil {
				log.Errorf("Write failed: %v", err)
			}
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "DELETE":
		log.Infof("Received request to unset key %s", key)
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

// Serve starts HTTP server
func Serve(kv *store.DistributedStore, port int) {
	srv = http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpAPI{
			store: kv,
		},
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// Shutdown terminates the HTTP
func Shutdown(ctx context.Context) {
	srv.Shutdown(ctx)
}
