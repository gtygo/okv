package kv

import "testing"

func TestGet(t *testing.T) {
	db := NewDB(*defaultOpts)
	testKv := Kv{
		Key:   []byte("test_k1"),
		Value: []byte("test_v1"),
	}

	db.Set(testKv)

	a, _ := db.Get([]byte("test_k1"))
	println(a)

}
