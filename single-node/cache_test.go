package singlenode

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	cache := newCache()
	_, err := cache.Get("key")
	if err == nil {
		t.Fatal("Expected an error when getting a non-existing key, got none")
	}
	if err.Error() != "Key key does not exist" {
		t.Fatalf("Expected error message to be 'Key key does not exist', gotten %s", err.Error())
	}
	var ttl float64 = 0.8
	cache.Set("key", "value", &ttl)
	idx, ok := cache.DataMap["key"]
	if !ok {
		t.Fatal("Expected 'key' to be found in cache.DataMap, not found")
	}
	if idx != 0 {
		t.Fatalf("Expected 0 to be the index of key, got %d", idx)
	}
	val, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Unexpected error when getting an existing key: %s", err.Error())
	}
	if val != "value" {
		t.Fatalf("Expected 'value' to be the value of 'key', got %v", val)
	}
	time.Sleep(1 * time.Second)
	_, err = cache.Get("key")
	if err == nil {
		t.Fatal("Expected an error when getting a expired key, got none")
	}
	var ttl1 float64 = 10
	entries := []CacheEntry{newCacheEntry("hello", "world", &ttl1), newCacheEntry("bye", "moon", nil)}
	// SetAll assigns the slice of CacheEntry to the underling data.
	cache.SetAll(entries)
	// The private method syncDataMap should have synced the data map
	idxHello, ok := cache.DataMap["hello"]
	if !ok {
		t.Fatal("Expected 'hello' to be found in cache.DataMap, not found")
	}
	if idxHello != 0 {
		t.Fatalf("Expected 0 to be the index of hello, got %d", idxHello)
	}
	idxBye, ok := cache.DataMap["bye"]
	if !ok {
		t.Fatal("Expected 'bye' to be found in cache.DataMap, not found")
	}
	if idxBye != 1 {
		t.Fatalf("Expected 1 to be the index of bye, got %d", idxBye)
	}
	// GetAll should now retrive the same data stored as entries
	cachedData := cache.GetAll()
	areEq := slices.EqualFunc(entries, cachedData, func(a CacheEntry, b CacheEntry) bool {
		if a.Ttl != nil && b.Ttl != nil {
			return a.Key == b.Key && a.Value == b.Value && *a.Ttl == *b.Ttl && a.Timestamp == b.Timestamp
		} else {
			return a.Key == b.Key && a.Value == b.Value && a.Timestamp == b.Timestamp
		}
	})
	if !areEq {
		t.Fatalf("Expected %v and %v to be equal, they are not", entries, cachedData)
	}
}

func TestCacheRaceConditionsGetSet(t *testing.T) {
	cache := newCache()

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id, j)
				cache.Set(key, "value", nil)
			}
		}(i)
	}

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id%5, j)
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestCacheRaceConditionsGetAllSetAll(t *testing.T) {
	cache := newCache()

	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []CacheEntry{}
			for j := range 100 {
				key := fmt.Sprintf("key-%d-%d", id, j)
				val := j
				data = append(data, newCacheEntry(key, val, nil))
				cache.SetAll(data)
			}
		}(i)
	}

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range 100 {
				cache.GetAll()
			}
		}(i)
	}

	wg.Wait()
}
