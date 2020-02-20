package kv

import (
	"github.com/gtygo/okv/bplustree"
	"github.com/gtygo/okv/engine"
)

type DB struct {
	Core engine.Engine
}

func NewDB() (*DB, error) {
	tree, err := bplustree.NewTree("my.db")
	if err != nil {
		return nil, err
	}
	return &DB{Core: tree}, nil
}
