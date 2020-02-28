package bitcask

import (
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"testing"
	"time"
)


var cfg=&Config{
	MaxFileSize:  DefaultMaxFileSize,
	MaxValueSize: DefaultMaxValueSize,
	FileDir:      "testDir",
}

func TestOkv_HugeData(t *testing.T) {
	putNum:=1000000

	if err:=os.RemoveAll(cfg.FileDir);err!=nil{
		fmt.Println("[start] remove test dir failed:",err)
	}
	bc,err:=Open(cfg)
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

	fmt.Printf("put done,time: %v.kv count: %v ",putEnd.Sub(putStart),putNum)

/*	for i:=0;i<putNum;i++{
		k:="key"+strconv.Itoa(i)
		v,err:=bc.Get([]byte(k))
		if err!=nil{
			fmt.Printf("got error when find key: %v, num: %v",err,i)
		}

	}

	*/
}

func TestBoltDb_HugeData(t *testing.T){
	putNum:=100000
	os.Remove("my.db")
	db, _ := bolt.Open("my.db", 0600, nil)
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
			return nil
		})

	putEnd:=time.Now()

	fmt.Printf("put done,time: %v.kv count: %v ",putEnd.Sub(putStart),putNum)

	//读数据
/*	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("block"))
		if bucket == nil{
			log.Fatal(err)//不存在
		}
		res := bucket.Get([]byte("333"))
		fmt.Println(string(res))
		return nil
	})*/
}


