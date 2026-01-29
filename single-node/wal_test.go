package singlenode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestNewWalFile(t *testing.T) {
	// with exsting file
	fl, err := newWalFile("../testfiles/app.log")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	if fl.lastDataLen != 0 {
		t.Fatalf("Expected initial data length to be 0, got %d", fl.lastDataLen)
	}
	if fl.numUpdated != 0 {
		t.Fatalf("Expected initial numUpdated to be 0, got %d", fl.numUpdated)
	}
	if fl.FilePath != "../testfiles/app.log" {
		t.Fatalf("Expected file path to be '../testfiles/app.log', got %s", fl.FilePath)
	}
	err = os.Remove("../testfiles/wal.jsonl")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal(err)
	}
	// with non-existing file (creates it)
	_, err = newWalFile("../testfiles/wal.jsonl")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	_, err = os.OpenFile("../testfiles/wal.jsonl", os.O_RDONLY, 0644)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		t.Fatalf("File `../testfiles/wal.jsonl` should not exist")
	}
}

func TestWriteRecord(t *testing.T) {
	err := os.Remove("../testfiles/wal.jsonl")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal(err)
	}
	wal, err := newWalFile("../testfiles/wal.jsonl")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	var ttl float64 = 10
	err = wal.WriteRecord("key", "value", &ttl)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	content, err := os.ReadFile("../testfiles/wal.jsonl")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	var data CacheEntry
	err = json.Unmarshal(content, &data)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	if data.Key != "key" || data.Value != "value" || *data.Ttl != ttl {
		t.Fatalf("Got unexpected CacheEntry{Key: %s, Value: %v, Ttl: %f}", data.Key, data.Value, *data.Ttl)
	}
}

func TestReadDataToEntries(t *testing.T) {
	err := os.Remove("../testfiles/wal.jsonl")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal(err)
	}
	var ttl float64 = 10
	entries := []CacheEntry{
		newCacheEntry("hello", "world", &ttl),
		newCacheEntry("bailando", "bailando", &ttl),
		newCacheEntry("test", "this", &ttl),
	}
	wal, _ := newWalFile("../testfiles/wal.jsonl")
	for _, entry := range entries {
		_ = wal.WriteRecord(entry.Key, entry.Value, entry.Ttl)
	}
	data, err := wal.ReadToEntries()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	areEq := slices.EqualFunc(entries, data, func(a CacheEntry, b CacheEntry) bool {
		if a.Ttl != nil && b.Ttl != nil {
			return a.Key == b.Key && a.Value == b.Value && *a.Ttl == *b.Ttl && a.Timestamp == b.Timestamp
		} else {
			return a.Key == b.Key && a.Value == b.Value && a.Timestamp == b.Timestamp
		}
	})
	if !areEq {
		t.Fatalf("Expected %v and %v to be equal, they are not", entries, data)
	}
}

func TestDedup(t *testing.T) {
	err := os.Remove("../testfiles/wal.jsonl")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal(err)
	}
	var ttl float64 = 10
	var anotherTtl float64 = 0.1
	entries := []CacheEntry{
		newCacheEntry("hello", "world", &ttl),
		newCacheEntry("hello", "world1", &ttl),
		newCacheEntry("test", "this", &anotherTtl),
	}
	wal, _ := newWalFile("../testfiles/wal.jsonl")
	for _, entry := range entries {
		_ = wal.WriteRecord(entry.Key, entry.Value, entry.Ttl)
		// sleep 0.3 seconds to space between entries
		time.Sleep(1 * time.Second)
	}
	// sleep 0.3 seconds
	time.Sleep(300 * time.Millisecond)
	err = wal.Dedup()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	data, err := wal.ReadToEntries()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	dedupEntries := []CacheEntry{entries[1]}
	areEq := slices.EqualFunc(dedupEntries, data, func(a CacheEntry, b CacheEntry) bool {
		if a.Ttl != nil && b.Ttl != nil {
			return a.Key == b.Key && a.Value == b.Value && *a.Ttl == *b.Ttl
		} else {
			return a.Key == b.Key && a.Value == b.Value
		}
	})
	if !areEq {
		t.Fatalf("Expected %v and %v to be equal, they are not", dedupEntries, data)
	}
}

func TestWalRaceConditions(t *testing.T) {
	err := os.Remove("../testfiles/wal.jsonl")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal(err)
	}
	wal, _ := newWalFile("../testfiles/wal.jsonl")

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id, j)
				val := j
				_ = wal.WriteRecord(key, val, nil)
			}
		}(i)
	}

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range 100 {
				_, _ = wal.ReadToEntries()
			}
		}(i)
	}

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range 100 {
				_ = wal.Dedup()
			}
		}(i)
	}

	wg.Wait()
}
