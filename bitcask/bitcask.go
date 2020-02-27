package bitcask

import (
	"os"
	"sync"
)

type BitCask struct{
	cfg *Config

	//used file for db
	activeFile *File
	oldFiles *Files
	lockFile *os.File

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
		rw:         ,
	}




}

func (bc *BitCask)Put(key []byte,value []byte)error{

}

func (bc *BitCask)Get(key []byte)([]byte,error){

}

func (bc *BitCask)Del(key []byte)error{


}

func (bc *BitCask)Close(){

}





