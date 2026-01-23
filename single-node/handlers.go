package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func handlePost(walfile *WalFile, w http.ResponseWriter, r *http.Request) {
	var req SetRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(
			w,
			fmt.Sprintf("An error occurred while reading your request: %s", err.Error()),
			http.StatusBadRequest,
		)
		return
	}
	err = walfile.WriteRecord(req.Key, req.Value, req.Ttl)
	if err != nil {
		http.Error(
			w,
			fmt.Sprintf("An error occurred while recording your entry: %s", err.Error()),
			http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func handleGet(cache *Cache, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(
			w,
			"Provided key was empty, please provide a non-empty key",
			http.StatusBadRequest,
		)
		return
	}
	val, err := cache.Get(key)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusNotFound,
		)
		return
	}
	apiResponse := GetResponse{Value: val}
	w.Header().Set("Content-Type", "application/json")
	j, err := json.Marshal(apiResponse)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func CreateHandler(cache *Cache, walfile *WalFile) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /cache", func(w http.ResponseWriter, r *http.Request) {
		handlePost(walfile, w, r)
	})

	mux.HandleFunc("GET /cache/{key}", func(w http.ResponseWriter, r *http.Request) {
		handleGet(cache, w, r)
	})

	return mux
}
