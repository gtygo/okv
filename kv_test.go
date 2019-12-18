package okv

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	o:=Options{
		Path:         "./",
		FilePerm:     0,
		CacheSizeMax: 0,
	}
	db:=New(o)




	fmt.Println(db)
}
