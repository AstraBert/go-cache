package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
)

type WalFile struct {
	mu       sync.RWMutex
	FilePath string
}

func newWalFile(filePath string) (*WalFile, error) {
	return &WalFile{
		FilePath: filePath,
	}, nil
}

func (f *WalFile) WriteRecord(key string, value any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	data := map[string]any{key: value}
	bindata, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = file.Write(append(bindata, '\n'))
	return err
}

func (f *WalFile) ReadToMap() (map[string]any, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// FIX: Actually read the file contents
	readData, err := os.ReadFile(f.FilePath)
	if err != nil {
		return nil, err
	}

	readDataStr := string(readData)
	lines := strings.Split(readDataStr, "\n")
	data := map[string]any{}

	for _, line := range lines {
		if line == "" {
			continue // Skip empty lines
		}
		var m map[string]any
		err = json.Unmarshal([]byte(line), &m)
		if err != nil {
			continue // Skip malformed lines
		}
		for k := range m {
			data[k] = m[k]
		}
	}
	return data, nil
}
