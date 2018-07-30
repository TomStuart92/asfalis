package main

import (
	"net/http"

	core "github.com/TomStuart92/asfalis/pkg/core"
	log "github.com/TomStuart92/asfalis/pkg/log"
)

func insert(w http.ResponseWriter, r *http.Request) {
	response, err := core.Insert("Hello", "World")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write([]byte(response.Key))
}

func get(w http.ResponseWriter, r *http.Request) {
	response, err := core.GetRecord("Hello")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write([]byte(response.Value))
}

func main() {
	log.Initialize()
	http.HandleFunc("/insert", insert)
	http.HandleFunc("/get", get)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
