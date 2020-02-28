package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/gtygo/okv/bitcask"
	"os"
	"strconv"
	"time"
)

func testBoltDB(putNum int){

	os.Remove("my.db")
	db, _ := bolt.Open("my.db", 0600, bolt.DefaultOptions)
	defer db.Close()

	testKTable:=make([][]byte,putNum)
	testVTable:=make([][]byte,putNum)

	for i:=0;i<putNum;i++{
		testKTable[i]=[]byte("key"+strconv.Itoa(i))
		testVTable[i]=[]byte("value"+strconv.Itoa(i))
	}
	fmt.Println("prepare done ,start run !")

	putStart:=time.Now()

	db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("block"))
		if bucket == nil {
			//创建bucket
			bucket, _ = tx.CreateBucket([]byte("block"))
		}
		for i:=0;i<putNum;i++ {
			if 	err:=bucket.Put(testKTable[i], testVTable[i]);err!=nil{
				fmt.Println("put error:",err,i)
			}
		}
		fmt.Println("put done, start commit ")
		return nil
	})

	putEnd:=time.Now()

	fmt.Printf("put to boltdb done,time: %v.kv count: %v ",putEnd.Sub(putStart),putNum)
	os.Remove("my.db")
}

func testOkvDB(putNum int){

	cfg:=&bitcask.Config{
		MaxFileSize:  bitcask.DefaultMaxFileSize,
		MaxValueSize: bitcask.DefaultMaxValueSize,
		FileDir:      "testDir",
	}

	if err:=os.RemoveAll(cfg.FileDir);err!=nil{
		fmt.Println("[start] remove test dir failed:",err)
	}
	bc,err:=bitcask.Open(cfg)
	if err!=nil{
		fmt.Println("open filed: ",err)
	}
	testKTable:=make([][]byte,putNum)
	testVTable:=make([][]byte,putNum)

	for i:=0;i<putNum;i++{
		testKTable[i]=[]byte("key"+strconv.Itoa(i))
		testVTable[i]=[]byte("value"+strconv.Itoa(i))
	}
	fmt.Println("prepare done ,start run !")

	putStart:=time.Now()
	for i:=0;i<putNum;i++{
		err:=bc.Put(testKTable[i],testVTable[i])
		if err!=nil{
			fmt.Println(err)
		}
	}
	putEnd:=time.Now()

	fmt.Printf("put to okv done,time: %v.kv count: %v ",putEnd.Sub(putStart),putNum)
	bc.Close()
	if err:=os.RemoveAll(cfg.FileDir);err!=nil{
		fmt.Println("[end] remove test dir failed:",err)
	}
}

func main(){
	putNum:=1000000
	//testBoltDB(putNum)
	testOkvDB(putNum)
}



