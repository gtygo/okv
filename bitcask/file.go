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

func (fs *Files)getFilePtr(fileId uint32)*File{
	fs.rw.RLock()
	defer fs.rw.RUnlock()
	return fs.fileCol[fileId]
}

func (fs *Files)putFilePtr(f *File,fileId uint32){
	fs.rw.Lock()
	defer fs.rw.Unlock()
	fs.fileCol[fileId]=f
}

func (fs *Files)closeAllFile(){
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


