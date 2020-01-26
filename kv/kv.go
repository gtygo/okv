package kv

import (
	"github.com/gtygo/okv/btree"
	"strings"
)

type Kv struct {
	Key []byte
	Value []byte
}

func (kv *Kv)Less(item btree.Item)bool{
	if v,ok:=item.(*Kv);ok{
		if strings.Compare(string(kv.Key),string(v.Key))==-1{
			return true
		}
	}
	return false
}
