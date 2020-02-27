package bitcask

import (
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

var once sync.Once
var mu =sync.RWMutex{}
var hashTable *HashTable

type HashTable struct{
	items map[string]*fileItem
}

func newHashTable()*HashTable{
	once.Do(func() {
		if hashTable==nil{
			hashTable=&HashTable{
				items:make(map[string]*fileItem),
			}
		}
	})
	return hashTable
}

func (ht *HashTable)get(key string)*fileItem{
	mu.Lock()
	defer mu.Unlock()
	return ht.items[key]
}

func (ht *HashTable)set(key string,fi *fileItem){
	mu.Lock()
	defer mu.Unlock()
	ht.items[key]=fi
}

func (ht *HashTable)del(key string){
	mu.Lock()
	defer mu.Unlock()
	delete(ht.items,key)
}

func (ht *HashTable)parseHintFile(hintFiles []*os.File){
	b:=make([]byte,HintSizeWithoutK,HintSizeWithoutK)

	for _,f:=range hintFiles{
		offset:=int64(0)
		fileName:=f.Name()
		l:=strings.LastIndex(fileName,"/")+1
		r:=strings.LastIndex(fileName,".hint")
		fileId,_:=strconv.ParseInt(fileName[l:r],10,32)

		for{
			n,err:=f.ReadAt(b,offset)
			if err!=nil&&err!=io.EOF{
				panic(err)
			}
			if err==io.EOF{
				break
			}
			if n!=HintSizeWithoutK{
				panic(n)
			}
			offset+=int64(n)
			tStamp,kSize,vSize,vOffset:=decodeHintFile(b)

			if kSize+vSize==0{
				continue
			}
			keyByte:=make([]byte,kSize)

			n,err=f.ReadAt(keyByte,offset)
			if err!=nil&&err!=io.EOF{
				panic(err)
			}

			if err==io.EOF{
				break
			}

			if n!=int(kSize){
				panic(n)
			}

			key:=string(keyByte)

			item:=&fileItem{
				fileId:uint32(fileId),
				valueSize:vSize,
				valueOffset:vOffset,
				timeStamp:tStamp,
			}
			offset+=int64(kSize)

			ht.set(key,item)
		}
	}
}















