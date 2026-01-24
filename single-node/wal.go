package singlenode

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type WalFile struct {
	mu          sync.RWMutex
	FilePath    string
	lastDataLen int
	numUpdated  int
}

func newWalFile(filePath string) (*WalFile, error) {
	_, err := os.Stat(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			file, err := os.OpenFile(filePath, os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			err = file.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	return &WalFile{
		FilePath:    filePath,
		lastDataLen: 0,
	}, nil
}

func (f *WalFile) WriteRecord(key string, value any, ttl *float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	data := newCacheEntry(key, value, ttl)
	bindata, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = file.Write(append(bindata, '\n'))
	return err
}

func (f *WalFile) ReadToEntries() ([]CacheEntry, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	readData, err := os.ReadFile(f.FilePath)
	if err != nil {
		return nil, err
	}

	readDataStr := string(readData)
	lines := strings.Split(readDataStr, "\n")
	data := make([]CacheEntry, 0, len(lines))
	potentialCorrupted := 0

	for _, line := range lines {
		if line == "" {
			continue // Skip empty lines
		}
		var m CacheEntry
		err = json.Unmarshal([]byte(line), &m)
		if err != nil {
			potentialCorrupted++
			continue // Skip malformed lines
		}
		data = append(data, m)
	}
	if potentialCorrupted > 0 && potentialCorrupted >= len(lines)/10 {
		log.Printf("There were %d errors over %d lines while deserializing\n", potentialCorrupted, len(lines))
	}
	return data, nil
}

func (f *WalFile) Dedup() error {
	data, err := f.ReadToEntries()
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// descending order by timestamp (newest first)
	sort.Slice(data, func(i, j int) bool {
		return data[i].Timestamp > data[j].Timestamp
	})
	towrite := []byte{}
	processedKeys := map[string]int64{}
	for _, entry := range data {
		// if the entry is expired, skip
		if entry.Ttl != nil {
			now := time.Now().Unix()
			if float64((now - entry.Timestamp)) > *entry.Ttl {
				continue
			}
		}
		// if the an already processed entry is more recent, skip
		timestamp, ok := processedKeys[entry.Key]
		if ok && timestamp > entry.Timestamp {
			continue
		}
		processedKeys[entry.Key] = entry.Timestamp
		bytedata, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		bytedata = append(bytedata, '\n')
		towrite = append(towrite, bytedata...)
	}
	if len(processedKeys) == f.lastDataLen && f.numUpdated != 0 {
		return nil
	}
	f.lastDataLen = len(processedKeys)
	f.numUpdated++
	return os.WriteFile(f.FilePath, towrite, 0644)
}
