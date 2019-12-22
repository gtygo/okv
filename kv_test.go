package okv

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	o := Options{
		Path:         "./",
		FilePerm:     0,
		CacheSizeMax: 0,
	}
	db := New(o)
	fmt.Println(db)
}

func TestSet(t *testing.T) {
	o := Options{
		Path:         "data",
		FilePerm:     6000,
		CacheSizeMax: 0,
	}
	db := New(o)

	err := db.Set("a", []byte("b4t5y6u7j6hyfvecdfju76hy5gt4frew"))
	if err != nil {
		t.Error(err)
	}
}
