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
	activeFile singleFileReader
	oldFiles MultiFileReader
	lockFile *os.File

	//hash table for memory
	hashTable *HashTable

	rw *sync.RWMutex
}

func Open(cfg *Config)(*BitCask,error){
	if cfg==nil{
		cfg=initConfig(DefaultMaxFileSize,DefaultFileDir)
	}

	bitCask:=&BitCask{
		cfg:        cfg,
		oldFiles:   newFiles(),
		rw:         &sync.RWMutex{},
		hashTable:newHashTable(),
	}

	_, err := os.Stat(cfg.FileDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if os.IsNotExist(err) {
		err = os.Mkdir(cfg.FileDir, 0755)
		if err != nil {
			return nil, err
		}
	}

	bitCask.lockFile,_=lockFile(cfg.FileDir+"/"+lockFileName)

	hintfiles,_:=bitCask.getHintFilePtrArr()

	bitCask.hashTable.parseHintFile(hintfiles)

	fileId,lastHintFile:=getLastHintFile(hintfiles)


	fileId,activeFile:=newActiveFile(fileId,cfg.FileDir)

	lastHintFile=newHintFile(fileId,cfg.FileDir)

	closeUnusedHintFile(hintfiles,fileId)

	activeFileStat,_:=activeFile.Stat()

	bitCask.activeFile=&File{
		fileId:fileId,
		file:activeFile,
		hintFile:lastHintFile,
		Offset:uint64(activeFileStat.Size()),
	}
	writePID(bitCask.lockFile,fileId)
	return bitCask,nil
}

func (bc *BitCask)Put(key []byte,value []byte)error{
	bc.rw.Lock()
	defer bc.rw.Unlock()
	//检查file是否可继续追加，如果不能则重新申请一个文件并复制到active file中
	checkWriteableFile(bc)

	item,err:=bc.activeFile.Write(key,value)
	if err!=nil{
		return err
	}
	bc.hashTable.set(string(key),&item)
	return nil
}

func (bc *BitCask)Get(key []byte)([]byte,error){
	item:=bc.hashTable.get(string(key))

	if item==nil{
		return nil,ErrNotFound
	}
	fileId:=item.fileId

	f,err:=bc.getFileState(fileId)
	if err!=nil&&os.IsNotExist(err){
		return nil,err
	}
	return f.Read(item.valueOffset,item.valueSize)
}

func (bc *BitCask)Del(key []byte)error{
	bc.rw.Lock()
	defer bc.rw.Unlock()

	if bc.activeFile==nil{
		return ErrReadFailed
	}
	item:=bc.hashTable.get(string(key))
	if item==nil{
		return ErrNotFound
	}

	checkWriteableFile(bc)
	if err:=bc.activeFile.Delete(key);err!=nil{
		return err
	}

	bc.hashTable.del(string(key))

	return nil
}

func (bc *BitCask)Close(){
	bc.activeFile.CloseAll()
	bc.oldFiles.CloseAllFile()
	bc.lockFile.Close()
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

func (bc *BitCask) getFileState(fileID uint32) (singleFileReader, error) {
	// lock up it from write able file
	if fileID == bc.activeFile.GetFileId() {
		return bc.activeFile, nil
	}
	// if not exits in write able file, look up it from OldFile
	bf := bc.oldFiles.GetFilePtr(fileID)
	if bf != nil {
		return bf, nil
	}

	bf, err := OpenFile(bc.cfg.FileDir, int(fileID))
	if err != nil {
		return nil, err
	}
	bc.oldFiles.PutFilePtr(bf, fileID)
	return bf, nil
}


