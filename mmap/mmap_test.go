package mmap

import (
	"fmt"
	"os"
	"testing"
)

func TestMmapWrite(t *testing.T) {
	os.Remove("mmap")
	f,_:=os.OpenFile("mmap",os.O_CREATE|os.O_RDWR,0644)
	defer f.Close()
	f.Truncate(32)

	data:=[]byte{1,8,9,4,2,1,6,7}
	err:=WriteData(f,0,data)
	if err!=nil{
		fmt.Println(err)
		return
	}
	data=[]byte{1,8,9,4,2,1,6,7}
	err=WriteData(f,8,data)
	if err!=nil{
		fmt.Println(err)
		return
	}
	ans:=make([]byte,16)
	f.Read(ans)
	fmt.Println("write ans:",ans)

}

func TestMmapRead(t *testing.T){
	f,_:=os.OpenFile("mmap",os.O_CREATE|os.O_RDWR,0644)
	defer f.Close()

	ans:=make([]byte,8)

	err:=ReadData(f,0,4,&ans)
	if err!=nil{
		fmt.Println(err)
		return
	}
	fmt.Println("ans: ",ans)

	err=ReadData(f,1,4,&ans)
	if err!=nil{
		fmt.Println(err)
		return
	}
	fmt.Println("ans: ",ans)

}