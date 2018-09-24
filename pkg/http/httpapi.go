package http

import (
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/TomStuart92/asfalis/pkg/store"
)

type httpAPI struct {
	store *store.Store
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
		h.store.Set(key, string(value))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if value, ok := h.store.Get(key); ok {
			w.Write([]byte(value))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "DELETE":
		if ok, err := h.store.Delete(key); ok {
			w.WriteHeader(http.StatusNoContent)
			return
		} else {
			log.Printf("Failed to DELETE (%v)\n", err)
			http.Error(w, "Failed to DELETE", http.StatusNotFound)
		}
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHTTP starts HTTP server
func ServeHTTP(port int, store *store.Store) {
	server := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: &httpAPI{store: store},
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}
