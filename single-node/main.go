package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"slices"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	Ttl   *int   `json:"ttl"`
}

type GetResponse struct {
	Value any `json:"value"`
}

func main() {
	cache := newCache()
	walfile, err := newWalFile("wal.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	server := http.NewServeMux()
	server.HandleFunc("POST /cache", func(w http.ResponseWriter, r *http.Request) {
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
	})
	server.HandleFunc("GET /cache/{key}", func(w http.ResponseWriter, r *http.Request) {
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
	})
	go func() {
		for {
			data, err := walfile.ReadToEntries()
			if err != nil {
				log.Printf("An error occurred while trying to read WAL file: %s\n", err.Error())
				continue
			}
			cachedData := cache.GetAll()
			if slices.EqualFunc(data, cachedData, func(a CacheEntry, b CacheEntry) bool {
				return a.Key == b.Key && a.Value == b.Value && *a.Ttl == *b.Ttl && a.Timestamp == b.Timestamp
			}) {
				continue
			} else {
				cache.SetAll(data)
				log.Println("Synced in-memory cache with WAL file")
			}
		}
	}()

	go func() {
		for {
			err := walfile.Dedup()
			if err != nil {
				log.Printf("An error occurred during deduplication: %s\n", err.Error())
				continue
			}
		}
	}()
	log.Print("starting server on :8000")

	err = http.ListenAndServe(":8000", server)
	log.Fatal(err)
}
