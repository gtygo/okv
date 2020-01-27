package kv

import (
	"github.com/gtygo/okv/btree"
	"github.com/pkg/errors"
)

var defaultOpts = &Options{
	M:    10,
	Name: "my.db",
}

type DB struct {
	tree *btree.BTree
}
type Options struct {
	M    int
	Name string
}

func NewDB(ops Options) *DB {
	return &DB{tree: btree.New(ops.M)}
}

func (db *DB) Set(kv Kv) error {
	if kv.Key == nil || kv.Value == nil {
		return errors.New("key or value is nil")
	}
	item := db.tree.ReplaceOrInsert(&kv)
	if item == nil {
		return errors.New("insert failed")
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	item := db.tree.Get(&Kv{
		Key: key,
	})

	if kv, ok := item.(*Kv); ok {
		return kv.Value, nil
	}
	return nil, errors.New("key not found")
}

func (db *DB) Delete(key []byte) error {
	db.tree.Delete(&Kv{
		Key: key,
	})
	return nil
}
