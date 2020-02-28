package bitcask

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

/*
run:
go test -coverprofile=coverage.data

go tool cover -html=coverage.data -o coverage.html

*/

func TestOpen_Basic(t *testing.T) {

	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}
	os.RemoveAll(cfg.FileDir)

	bitCask,err:=Open(cfg)
	assert.Equal(t,err,nil)
	assert.NotEqual(t,bitCask.activeFile,nil)
	assert.NotEqual(t,bitCask.hashTable,nil)
	bitCask.Close()

	err=os.RemoveAll(cfg.FileDir)
}


func TestPut_Basic(t *testing.T){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}

	mockedActiveObj:=new(MockedSingleFileReader)
	mockedActiveObj.On("Write",[]byte("k1"),[]byte("v1")).Return(fileItem{},nil)
	mockedActiveObj.On("GetFileOffset",).Return(uint64(0))

	bitCask:=&BitCask{
		cfg:cfg,
		activeFile:mockedActiveObj,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}
	err:=bitCask.Put([]byte("k1"),[]byte("v1"))
	assert.Equal(t,err,nil)

}

func TestPut_ConerCase(t *testing.T){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}
	mockedActiveObj:=new(MockedSingleFileReader)
	mockedActiveObj.On("Write",[]byte("k1"),[]byte("v1")).Return(fileItem{},ErrReadFailed)
	mockedActiveObj.On("GetFileOffset",).Return(uint64(0))
	bitCask:=&BitCask{
		cfg:cfg,
		activeFile:mockedActiveObj,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}

	err:=bitCask.Put([]byte("k1"),[]byte("v1"))
	assert.Equal(t,err,ErrReadFailed)
}

func BenchmarkPut_Sequential(b *testing.B){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}

	mockedActiveObj:=new(MockedSingleFileReader)
	mockedActiveObj.On("Write",[]byte("k1"),[]byte("v1")).Return(fileItem{},nil)
	mockedActiveObj.On("GetFileOffset",).Return(uint64(0))

	bitCask:=&BitCask{
		cfg:cfg,
		activeFile:mockedActiveObj,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}

	for i:=0;i<b.N;i++{
		bitCask.Put([]byte("k1"),[]byte("v1"))
	}
}




func TestGet_Basic(t *testing.T){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}

	mockedActiveObj:=new(MockedSingleFileReader)
	mockedActiveObj.On("Write",[]byte("k1"),[]byte("v1")).Return(fileItem{fileId:0},nil)
	mockedActiveObj.On("GetFileOffset",).Return(uint64(0))
	mockedActiveObj.On("GetFileId",).Return(uint32(0))
	mockedActiveObj.On("Read",uint64(0),uint32(0)).Return([]byte("v1"),nil)

	bitCask:=&BitCask{
		cfg:cfg,
		activeFile:mockedActiveObj,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}
	bitCask.hashTable.items["k1"]=&fileItem{fileId:0,valueSize:0}
	_,err:=bitCask.Get([]byte("k1"))
	assert.Equal(t,err,nil)


}

func TestGet_Conercase(t *testing.T){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}

	mockedActiveObj:=new(MockedSingleFileReader)
	bitCask:=&BitCask{
		cfg:cfg,
		activeFile:mockedActiveObj,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}

	_,err:=bitCask.Get([]byte("k1"))
	assert.Equal(t,err,ErrNotFound)
}

func TestDelete_Basic(t *testing.T){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}

	mockedActiveObj:=new(MockedSingleFileReader)
	mockedActiveObj.On("Write",[]byte("k1"),[]byte("v1")).Return(fileItem{fileId:0},nil)
	mockedActiveObj.On("GetFileOffset",).Return(uint64(0))
	mockedActiveObj.On("GetFileId",).Return(uint32(0))
	mockedActiveObj.On("Read",uint64(0),uint32(0)).Return([]byte("v1"),nil)
	mockedActiveObj.On("Delete",[]byte("k1")).Return(nil)

	bitCask:=&BitCask{
		cfg:cfg,
		activeFile:mockedActiveObj,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}
	bitCask.hashTable.items["k1"]=&fileItem{fileId:0,valueSize:0}
	err:=bitCask.Del([]byte("k1"))
	assert.Equal(t,err,nil)
}

func TestDelete_Conercase(t *testing.T){
	cfg:=&Config{
		MaxFileSize:  DefaultMaxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:      "testDir",
	}
	bitCask:=&BitCask{
		cfg:cfg,
		rw:&sync.RWMutex{},
	}
	bitCask.hashTable=&HashTable{items:make(map[string]*fileItem)}
	err:=bitCask.Del([]byte("k1"))
	assert.Equal(t,err,ErrReadFailed)

	mockedActiveObj:=new(MockedSingleFileReader)
	bitCask.activeFile=mockedActiveObj
	err=bitCask.Del([]byte("k1"))
	assert.Equal(t,err,ErrNotFound)

	mockedActiveObj.On("Write",[]byte("k1"),[]byte("v1")).Return(fileItem{fileId:0},nil)
	mockedActiveObj.On("GetFileOffset",).Return(uint64(0))
	mockedActiveObj.On("GetFileId",).Return(uint32(0))
	mockedActiveObj.On("Read",uint64(0),uint32(0)).Return([]byte("v1"),nil)
	mockedActiveObj.On("Delete",[]byte("k1")).Return(ErrReadFailed)
	bitCask.hashTable.items["k1"]=&fileItem{fileId:0,valueOffset:0}

	err=bitCask.Del([]byte("k1"))
	assert.Equal(t,err,ErrReadFailed)

}


type MockedSingleFileReader struct{
	mock.Mock
}

func (m *MockedSingleFileReader)GetFileId()uint32{
	args:=m.Called()
	return args.Get(0).(uint32)
}

func (m *MockedSingleFileReader)GetFileOffset()uint64{
	args:=m.Called()
	return args.Get(0).(uint64)
}

func (m *MockedSingleFileReader)Read(a uint64,b uint32)([]byte,error){
	args:=m.Called(a,b)
	return args.Get(0).([]byte),args.Error(1)
}

func (m *MockedSingleFileReader)Write(k []byte,v []byte)(fileItem,error){
	args:=m.Called(k,v)
	return args.Get(0).(fileItem),args.Error(1)
}

func (m *MockedSingleFileReader)Delete(k []byte)error{
	args:=m.Called(k)
	return args.Error(0)
}

func (m *MockedSingleFileReader)CloseAll(){
}
