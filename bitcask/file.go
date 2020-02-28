package bitcask

import (
	"os"
	"strconv"
	"sync"
	"time"
)

/*
kv item:
----------------------------------------------------------
|  crc | t stamp |  ksz  | value_sz | key   |   value    |
---------------------------------------------------------
| 4bit |  4bit   | 4bit  |   4bit   | x bit |  xx bit    |
----------------------------------------------------------

hint:
 ----------------------------------------------------
 |t stamp |  ksz  | value_sz | value_pos   |  key   |
 ---------------------------------------------------
 | 4 bit  | 4 bit |  4 bit   |    8 bit    | x bit  |
 ----------------------------------------------------

*/

const(

	// 4*4
	ItemSizeWithoutKV = 1<<4

	// 4*3 + 8 =20
	HintSizeWithoutK = 20

)

// 定义fileReader抽象 1.file的抽象，这里的file其实就是activefile
type singleFileReader interface {
	GetFileId()uint32

	GetFileOffset()uint64

	Read(uint64,uint32)([]byte,error)

	Write([]byte,[]byte)(fileItem,error)

	Delete([]byte)error

	CloseAll()
}

//定义multifileReader抽象 这里的file其实是old file ，他的性质是只读，对外暴漏 getFilePtr,putFilePtr,closeAllFile三个方法

type MultiFileReader interface {
	GetFilePtr(uint32)*File
	PutFilePtr(*File,uint32)
	CloseAllFile()
}


//只读文件
type Files struct{
	//filCol用来管理所有只读文件
	fileCol map[uint32]*File
	rw *sync.RWMutex
}

func newFiles()*Files{
	return &Files{
		fileCol: make(map[uint32]*File),
		rw:      &sync.RWMutex{},
	}
}

func (fs *Files)GetFilePtr(fileId uint32)*File{
	fs.rw.RLock()
	defer fs.rw.RUnlock()
	return fs.fileCol[fileId]
}

func (fs *Files)PutFilePtr(f *File,fileId uint32){
	fs.rw.Lock()
	defer fs.rw.Unlock()
	fs.fileCol[fileId]=f
}

func (fs *Files)CloseAllFile(){
	fs.rw.Lock()
	defer fs.rw.Unlock()
	for _,f:=range fs.fileCol{
		f.file.Close()
		f.hintFile.Close()
	}
}


//可写文件


type File struct {
	file *os.File
	hintFile *os.File
	fileId uint32
	Offset uint64
}

func NewFile()*File{
	return &File{}
}

func OpenFile(fileDir string,fileId int)(*File,error){
	f,err:=os.OpenFile(fileDir+"/"+strconv.Itoa(fileId),os.O_RDONLY,os.ModePerm)
	if err!=nil{
		return nil,err
	}
	return &File{
		file:     f,
		hintFile: nil,
		fileId:   uint32(fileId),
		Offset:   0,
	},nil
}

//根据offset在文件中获取，并返回数据
func (af *File)Read(off uint64,dataLen uint32)([]byte,error){
	value:=make([]byte,dataLen)
	af.file.Seek(int64(off),0)
	_,err:=af.file.Read(value)
	if err!=nil{
		return nil,err
	}
	return value,err
}

func (af *File)Write(key []byte,value []byte)(fileItem,error){
	timeStamp :=uint32(time.Now().Unix())
	keySize:=uint32(len(key))
	valueSize:=uint32(len(value))
	itemBytes:=encodeItem(timeStamp,keySize,valueSize,key,value)
	itemSize:=ItemSizeWithoutKV+keySize+valueSize
	vOffset:=af.Offset+uint64(ItemSizeWithoutKV+keySize)

	_,err:=appendWriteFile(af.file,itemBytes)
	if err!=nil{
		panic(err)
	}
	af.Offset+=uint64(itemSize)
	return fileItem{
		fileId:af.fileId,
		valueSize:valueSize,
		valueOffset:vOffset,
		timeStamp:timeStamp,
	},nil
}

func (af *File)Delete(key []byte)error{

	// 1. write into datafile
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(0)
	valueSize := uint32(0)
	vec := encodeItem(timeStamp, keySize, valueSize, key, nil)
	//logger.Info(len(vec), keySize, valueSize)
	entrySize := ItemSizeWithoutKV + keySize + valueSize
	// TODO
	// race data
	valueOffset := af.Offset + uint64(ItemSizeWithoutKV+keySize)
	// write data file into disk
	// TODO
	// assert WriteAt function
	_, err := appendWriteFile(af.file, vec)
	if err != nil {
		panic(err)
	}

	//logger.Debug("has write into data file:", n)
	// 2. write hint file disk
	hintData := encodeHintFile(timeStamp, keySize, valueSize, valueOffset, key)

	// TODO
	// assert write function
	_, err = appendWriteFile(af.hintFile, hintData)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into hint file:", n)
	af.Offset += uint64(entrySize)
	return nil


}


func (af *File)CloseAll(){
	af.file.Close()
	af.hintFile.Close()
}

func (af *File)GetFileId()uint32{
	return af.fileId
}

func (af *File)GetFileOffset()uint64{
	return af.Offset
}