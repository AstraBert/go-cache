package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
)

type WalFile struct {
	mu   sync.RWMutex
	File *os.File
}

func newWalFile(filePath string) (*WalFile, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WalFile{
		File: file,
	}, nil
}

func (f *WalFile) WriteRecord(key string, value any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data := map[string]any{key: value}
	bindata, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = f.File.Write(append(bindata, '\n'))
	return err
}

func (f *WalFile) ReadToMap() (map[string]any, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var readData []byte
	_, err := f.File.Read(readData)
	if err != nil {
		return nil, err
	}
	readDataStr := string(readData)
	lines := strings.Split(readDataStr, "\n")
	data := map[string]any{}
	for _, line := range lines {
		var m map[string]any
		err = json.Unmarshal([]byte(line), &m)
		if err != nil {
			return nil, err
		}
		for k := range m {
			data[k] = m[k]
		}
	}
	return data, nil
}
