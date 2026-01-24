package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

const DEFAULT_GET_RATE_LIMIT = 10000
const DEFAULT_POST_RATE_LIMIT = 1000

func handlePost(walfile *WalFile, cache *Cache, w http.ResponseWriter, r *http.Request) {
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
	if len(cache.Data) == cache.MaxSize && cache.MaxSize > 0 {
		http.Error(
			w,
			"Cache has reached the max size: your entry has been queued.",
			http.StatusInsufficientStorage,
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

func getLimits() (int, int) {
	var getLimit int
	var postLimit int
	getStr, ok := os.LookupEnv("CACHE_RATE_LIMIT_GET")
	if !ok {
		getLimit = DEFAULT_GET_RATE_LIMIT
	} else {
		getInt, err := strconv.Atoi(getStr)
		if err != nil {
			getLimit = DEFAULT_GET_RATE_LIMIT
		} else {
			getLimit = getInt
		}
	}
	postStr, ok := os.LookupEnv("CACHE_RATE_LIMIT_SET")
	if !ok {
		postLimit = DEFAULT_POST_RATE_LIMIT
	} else {
		postInt, err := strconv.Atoi(postStr)
		if err != nil {
			postLimit = DEFAULT_POST_RATE_LIMIT
		} else {
			postLimit = postInt
		}
	}
	return getLimit, postLimit
}

func CreateHandler(cache *Cache, walfile *WalFile) http.Handler {
	getLimit, postLimit := getLimits()
	getLimiter := rate.NewLimiter(rate.Every(1*time.Minute), getLimit)
	postLimiter := rate.NewLimiter(rate.Every(1*time.Minute), postLimit)
	mux := http.NewServeMux()

	mux.HandleFunc("POST /cache", func(w http.ResponseWriter, r *http.Request) {
		if !postLimiter.Allow() {
			log.Println("Rate limit exceeded")
			http.Error(
				w,
				"Rate limit exceeded",
				http.StatusTooManyRequests,
			)
			return
		}
		handlePost(walfile, cache, w, r)
	})

	mux.HandleFunc("GET /cache/{key}", func(w http.ResponseWriter, r *http.Request) {
		if !getLimiter.Allow() {
			log.Println("Rate limit exceeded")
			http.Error(
				w,
				"Rate limit exceeded",
				http.StatusTooManyRequests,
			)
			return
		}
		handleGet(cache, w, r)
	})

	return mux
}
