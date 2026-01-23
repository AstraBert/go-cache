package main

import (
	"context"
	"log"
	"os"
	"slices"
	"time"
)

func WalSyncWorker(walfile *WalFile, cache *Cache, done <-chan os.Signal, ctx context.Context) {
	for {
		select {
		case <-done:
			log.Println("Stopping WAL sync worker...")
			return
		case <-ctx.Done():
			log.Println("Stopping WAL sync worker...")
			return
		default:
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
	}
}

func DedupWorker(walfile *WalFile, done <-chan os.Signal, ctx context.Context) {
	for {
		select {
		case <-done:
			log.Println("Stoping deduplication worker...")
			return
		case <-ctx.Done():
			log.Println("Stopping deduplication worker...")
			return
		default:
			err := walfile.Dedup()
			if err != nil {
				log.Printf("An error occurred during deduplication: %s\n", err.Error())
				continue
			}
			time.Sleep(1 * time.Second)

		}
	}
}
