package main

import (
	"fmt"
	"maps"
	"sync"
)

type NotExistError struct {
	key string
}

func (e NotExistError) Error() string {
	return fmt.Sprintf("Key %s does not exist", e.key)
}

type Cache struct {
	mu   sync.RWMutex
	Data map[string]any
}

func newNotExistError(key string) NotExistError {
	return NotExistError{key: key}
}

func newCache() *Cache {
	return &Cache{
		mu:   sync.RWMutex{},
		Data: map[string]any{},
	}
}

func (c *Cache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data[key] = value
}

func (c *Cache) Get(key string) (any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.Data[key]
	if !ok {
		return nil, newNotExistError(key)
	}
	return val, nil
}

func (c *Cache) GetAll() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return maps.Clone(c.Data)
}

func (c *Cache) SetAll(data map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	maps.Copy(c.Data, data)
}
