package okv

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

type Options struct {
	Path         string
	FilePerm     os.FileMode
	CacheSizeMax uint64
}

type KV struct {
	Options
	mu        sync.RWMutex
	cache     map[string][]byte
	cacheSize uint64
}

func New(o Options) *KV {

	kv := &KV{
		Options:   o,
		cache:     map[string][]byte{},
		cacheSize: 0,
	}
	return kv
}

func (kv *KV) Set(key string, val []byte) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.setStreamWithLock(key, bytes.NewReader(val), false)
}

func (kv *KV) setStreamWithLock(key string, r io.Reader, sync bool) error {

}

func (kv *KV) createKeyFileWithLock(key string) (*os.File, error) {
	mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	f, err := os.OpenFile(kv.Options.Path, mode, kv.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("chmod %s", err)
	}
	return f, nil

}
