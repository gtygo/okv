package mmap

import (
	"fmt"
	mmap "github.com/edsrzf/mmap-go"
	"os"
)

func WriteData(fd *os.File,offset int,data []byte)error{
	if err:=fd.Truncate(int64(offset + len(data)));err!=nil{
		return err
	}
	m,err:=mmap.Map(fd,mmap.RDWR,0)
	if err!=nil{
		fmt.Println("mmap error:",err)
		return err
	}
	defer m.Unmap()

	for i:=0;i<len(data);i++{
		m[offset]=data[i]
		offset++
	}
	m.Flush()
	return nil
}

func ReadData(fd *os.File,offset int,len int,ans *[]byte)error{
	m,err:=mmap.Map(fd,mmap.RDONLY,0)
	if err!=nil{
		return err
	}
	//fmt.Println("mmap 字节信息：",m)

	copy(*ans,m[offset:offset+len])

	m.Unmap()
	return nil
}