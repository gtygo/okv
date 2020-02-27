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
func (af *File)Read(off uint64,dataLen uint64)([]byte,error){
	value:=make([]byte,dataLen)
	af.file.Seek(int64(off),0)
	_,err:=af.file.Read(value)
	if err!=nil{
		return nil,err
	}
	return value,err
}

func (af *File)Write(key []byte,value []byte)error{
	timeStamp :=uint32(time.Now().Unix())
	keySize:=uint32(len(key))
	valueSize:=uint32(len(value))


	//todo: encode kvitem






}

func (af *File)Delete(key []byte)error{




}


