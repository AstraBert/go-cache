package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT)

	server := &http.Server{
		Addr:    ":8000",
		Handler: CreateHandler(cache, walfile),
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		WalSyncWorker(walfile, cache, done, ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		DedupWorker(walfile, done, ctx)
	}()

	// Start server in a goroutine
	go func() {
		log.Println("starting server on :8000")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %s\n", err)
		}
	}()

	<-done
	log.Println("Shutting down server and workers...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %s\n", err)
	}
	cancel()
	wg.Wait()
	log.Println("Application stopped")
}
