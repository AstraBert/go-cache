package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

const DEFAULT_MAX_SIZE int = 0

type NotExistError struct {
	key string
}

func (e NotExistError) Error() string {
	return fmt.Sprintf("Key %s does not exist", e.key)
}

type ExpiredEntryError struct {
	key     string
	ttl     int
	elapsed int64
}

func (e ExpiredEntryError) Error() string {
	return fmt.Sprintf("Entry %s has expired (ttl: %d, actual elapsed time: %d)", e.key, e.ttl, e.elapsed)
}

type CacheEntry struct {
	Key       string `json:"key"`
	Value     any    `json:"value"`
	Timestamp int64  `json:"timestamp"`
	Ttl       *int   `json:"ttl"`
}

type Cache struct {
	mu      sync.RWMutex
	Data    []CacheEntry
	DataMap map[string]int
	MaxSize int
}

func newNotExistError(key string) NotExistError {
	return NotExistError{key: key}
}

func newExpiredError(key string, ttl int, elapsed int64) ExpiredEntryError {
	return ExpiredEntryError{key: key, ttl: ttl, elapsed: elapsed}
}

func newCacheEntry(key string, value any, ttl *int) CacheEntry {
	now := time.Now().Unix()
	return CacheEntry{Key: key, Value: value, Ttl: ttl, Timestamp: now}
}

func newCache() *Cache {
	var maxSize int
	ms, ok := os.LookupEnv("MAX_CACHE_SIZE")
	if ok {
		typedMs, err := strconv.Atoi(ms)
		if err != nil {
			maxSize = DEFAULT_MAX_SIZE
		} else {
			maxSize = typedMs
		}
	} else {
		maxSize = DEFAULT_MAX_SIZE
	}
	return &Cache{
		mu:      sync.RWMutex{},
		Data:    []CacheEntry{},
		DataMap: map[string]int{},
		MaxSize: maxSize,
	}
}

func (c *Cache) Set(key string, value any, ttl *int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data = append(c.Data, newCacheEntry(key, value, ttl))
	c.DataMap[key] = len(c.Data) - 1
}

func (c *Cache) Get(key string) (any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.DataMap[key]
	if !ok {
		return nil, newNotExistError(key)
	}
	entry := c.Data[val]
	// handle the case where TTL-based cleanup has not yet been performed
	if entry.Ttl != nil {
		now := time.Now().Unix()
		elapsed := now - entry.Timestamp
		if elapsed > int64(*entry.Ttl) {
			return nil, newExpiredError(key, *entry.Ttl, elapsed)
		}
	}
	return entry.Value, nil
}

func (c *Cache) GetAll() []CacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.Data)
}

func (c *Cache) SetAll(data []CacheEntry) {
	c.mu.Lock()
	c.Data = data
	c.mu.Unlock()
	c.syncDataMap()
}

func (c *Cache) syncDataMap() {
	c.mu.Lock()
	defer c.mu.Unlock()
	dataMap := map[string]int{}
	for i, entry := range c.Data {
		dataMap[entry.Key] = i
	}
	c.DataMap = dataMap
}
