package bitcask

import (
	"os"
	"strings"
	"sync"
)

const (
	lockFileName ="bitcask.lock"
)

type BitCask struct{
	cfg *Config

	//used file for db
	activeFile *File
	oldFiles *Files
	lockFile *os.File

	//hash table for memory
	hashTable *HashTable

	rw *sync.RWMutex
}

func Open(cfg *Config)(*BitCask,error){
	if cfg==nil{
		cfg=initConfig(DefaultMaxFileSize,DefaultFileDir)
	}
	//check dir is exist
	bitCask:=&BitCask{
		cfg:        cfg,
		oldFiles:   newFiles(),
		rw:         &sync.RWMutex{},
		hashTable:newHashTable(),
	}

	bitCask.lockFile,_=os.OpenFile(cfg.FileDir+"/"+lockFileName,os.O_EXCL|os.O_CREATE|os.O_RDWR, os.ModePerm)

	//todo: scan hint file
	hintfiles,err:=bitCask.getHintFilePtrArr()
	if err!=nil{
		return nil,err
	}
	//todo: parse hint file
	bitCask.hashTable.parseHintFile(hintfiles)

	fileId,lastHintFile:=getLastHintFile(hintfiles)



	fileId,writeFile:=setWriteableFile(fileId,cfg.FileDir)


	lastHintFile=setHintFile(fileId,cfg.FileDir)

	closeUnusedHintFile(hintfiles,fileId)

	writeFileStat,_:=writeFile.Stat()

	bitCask.activeFile=&File{
		fileId:fileId,
		file:writeFile,
		hintFile:lastHintFile,
		Offset:uint64(writeFileStat.Size()),
	}
	writePID(bitCask.lockFile,fileId)

	return bitCask,nil
}

func (bc *BitCask)Put(key []byte,value []byte)error{
	bc.rw.Lock()
	defer bc.rw.Unlock()
	//检查file是否可写
	checkWriteableFile(bc)



	bc.activeFile.Write(key,value)

}

func (bc *BitCask)Get(key []byte)([]byte,error){

}

func (bc *BitCask)Del(key []byte)error{


}

func (bc *BitCask)Close(){

}


func (bc *BitCask)getHintFilePtrArr()([]*os.File,error){
	dirFilePtr,err:=os.OpenFile(bc.cfg.FileDir,os.O_RDONLY,os.ModeDir)
	if err!=nil{
		return nil,err
	}
	defer dirFilePtr.Close()
	lockName:=[]string{lockFileName}

	// find hint file
	names,err:=dirFilePtr.Readdirnames(-1)
	if err!=nil{
		return nil,err
	}
	hintFileNames:=make([]string,0,len(names))

	for _,x:=range names{
		if strings.Contains(x,"hint")&&!hasSuffix(x,lockName){
			hintFileNames=append(hintFileNames,x)
		}
	}

	hintFilePtrArr:=make([]*os.File,0,len(hintFileNames))
	for _,x:=range hintFileNames{
		if hasSuffix(x,lockName){
			continue
		}
		f,err:=os.OpenFile(bc.cfg.FileDir+"/"+x,os.O_RDONLY,0755)
		if err!=nil{
			return nil,err
		}
		hintFilePtrArr=append(hintFilePtrArr,f)
	}
	if len(hintFilePtrArr)==0{
		return nil,nil
	}
	return hintFilePtrArr,nil
}


