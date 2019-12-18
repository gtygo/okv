package okv

import (
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

