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
	Compression  Compression
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
	f, err := kv.createKeyFileWithLock(key)
	if err != nil {
		return fmt.Errorf("create key files: %s", err)
	}
	wc := io.WriteCloser(&noWriteCloser{f})
	if kv.Compression != nil {
		wc, err = kv.Compression.Writer(f)
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			return fmt.Errorf("comprression writer: %s", err)
		}
	}

	if _, err := io.Copy(wc, r); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("i/o cpopy: %s", err)
	}

	if err := wc.Close(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("compression close: %s", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("file close: %s", err)
	}
	return nil
}

func (kv *KV) createKeyFileWithLock(key string) (*os.File, error) {
	mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	f, err := os.OpenFile(kv.Options.Path, mode, kv.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("chmod %s", err)
	}
	return f, nil
}

type noWriteCloser struct {
	io.Writer
}

func (wc *noWriteCloser) Write(p []byte) (int, error) {
	return wc.Writer.Write(p)
}

func (wc *noWriteCloser) Close() error {
	return nil
}
