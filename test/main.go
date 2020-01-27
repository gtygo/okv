package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

const (
	NIL_OFFSET=0xdeadbeef
	MAX_FREEBLOCK=500
)
type Tree struct{
	OffSet uint64
	NodePool *sync.Pool
	FreeBlocks []uint64
	File *os.File
	BlockSize uint64
	FileSize uint64
}

type Node struct{
	IsDiskInTree bool
	Children []uint64
	Self uint64
	Next uint64
	Prev uint64
	Parent uint64
	Keys []string
	Records []string
	IsLeaf bool
}

func NewTree()(*Tree,error){
	var stat syscall.Statfs_t
	var fstat os.FileInfo

	f,err:=os.OpenFile("my.db",os.O_CREATE|os.O_RDWR,0644)
	if err!=nil{
		return nil,err
	}

	if err = syscall.Statfs("my.db", &stat); err != nil {
		return nil, err
	}
	blockSize := uint64(stat.Bsize)

	if fstat, err = f.Stat(); err != nil {
		return nil, err
	}

	return &Tree{
		OffSet:     NIL_OFFSET,
		NodePool:   &sync.Pool{New: func() interface{} {
			return &Node{}
		}},
		FreeBlocks: make([]uint64,0,MAX_FREEBLOCK),
		File:       f,
		BlockSize:  blockSize,
		FileSize: uint64(fstat.Size()),
	},nil
}

func (t *Tree)Close()error{
	if t.File!=nil{
		t.File.Sync()
		return t.File.Close()
	}
	return nil
}

func (t *Tree)Insert(key string,val string)error{
	if t.OffSet==NIL_OFFSET{
		println("start insert offset is zero")
		node,err:=t.newNodeFromDisk()
		if err!=nil{
			return err
		}
		t.OffSet=node.Self
		node.IsDiskInTree=true
		node.Keys=append(node.Keys,key)
		node.Records=append(node.Records,val)
		node.IsLeaf=true
		return t.flushAndPushNodePool(node)
	}
	return t.InsertToLeaf(key,val)
}


func (t *Tree)newNodeFromDisk()(*Node,error){

	node:=t.NodePool.Get().(*Node)
	if len(t.FreeBlocks)>0{
		off:=t.FreeBlocks[0]
		t.FreeBlocks=t.FreeBlocks[1:len(t.FreeBlocks)]
		t.initNodeForUsage(node)
		node.Self=off
		return node,nil
	}
	if err:=t.checkDiskBlock();err!=nil{
		return nil,err
	}
	if len(t.FreeBlocks)>0{
		off:=t.FreeBlocks[0]
		t.FreeBlocks=t.FreeBlocks[1:len(t.FreeBlocks)]
		t.initNodeForUsage(node)
		node.Self=off
		return node,nil
	}
	return nil,errors.New("can not alloc node")
}

func (t *Tree)checkDiskBlock()error{
	node:=&Node{}

	bs:=t.BlockSize
	for i:=uint64(0);i<t.FileSize&&len(t.FreeBlocks)<MAX_FREEBLOCK;i+=bs{
		if i+bs>t.FileSize{
			break
		}
		if err:=t.seekNode(node,i);err!=nil{
			return err
		}
		if !node.IsLeaf{
			t.FreeBlocks=append(t.FreeBlocks,i)
		}
	}
	nextFile:=((t.FileSize+4095)/4096)*4096
	for len(t.FreeBlocks)<MAX_FREEBLOCK{
		t.FreeBlocks=append(t.FreeBlocks,nextFile)
		nextFile+=bs
	}
	t.FileSize=nextFile
	return nil
}


func (t *Tree)seekNode(node *Node,off uint64)error{
	t.clearNodeForUsage(node)


	b:=make([]byte,8)
	if _,err:=t.File.ReadAt(b,int64(off));err!=nil{
		return err
	}

	buf:=bytes.NewBuffer(b)

	var dataLen uint64

	if err:=binary.Read(buf,binary.LittleEndian,&dataLen);err!=nil{
		return err
	}

	b=make([]byte,dataLen)
	if _,err:=t.File.ReadAt(b,int64(off)+8);err!=nil{
		return err
	}

	buf=bytes.NewBuffer(b)

	//is disk in tree
	if err:=binary.Read(buf,binary.LittleEndian,&node.IsDiskInTree);err!=nil{
		return err
	}

	//children count
	childCount:=0
	if err:=binary.Read(buf,binary.LittleEndian,&childCount);err!=nil{
		return err
	}

	node.Children=make([]uint64,childCount)

	for i:=0;i<childCount;i++{
		child:=uint64(0)
		if err:=binary.Read(buf,binary.LittleEndian,&child);err!=nil{
			return err
		}
		node.Children[i]=child
	}

	//self
	if err:=binary.Read(buf,binary.LittleEndian,&node.Self);err!=nil{
		return err
	}

	//next
	if err:=binary.Read(buf,binary.LittleEndian,&node.Next);err!=nil{
		return err
	}

	//prev
	if err:=binary.Read(buf,binary.LittleEndian,&node.Prev);err!=nil{
		return err
	}

	//parent
	if err:=binary.Read(buf,binary.LittleEndian,&node.Parent);err!=nil{
		return err
	}

	//keys
	keysCount:=uint8(0)
	if err:=binary.Read(buf,binary.LittleEndian,&keysCount);err!=nil{
		return err
	}
	node.Keys=make([]string,keysCount)
	for i:=uint8(0);i<keysCount;i++{
		l:=uint8(0)
		if err:=binary.Read(buf,binary.LittleEndian,&l);err!=nil{
			return err
		}
		v:=make([]byte,l)
		if err:=binary.Read(buf,binary.LittleEndian,&v);err!=nil{
			return err
		}
		node.Keys[i]=string(v)
	}
	// Records
	recordCount := uint8(0)
	if err := binary.Read(buf, binary.LittleEndian, &recordCount); err != nil {
		return err
	}
	node.Records = make([]string, recordCount)
	for i := uint8(0); i < recordCount;i++ {
		l := uint8(0)
		if err := binary.Read(buf, binary.LittleEndian, &l); err != nil {
			return err
		}
		v := make([]byte, l)
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return err
		}
		node.Records[i] = string(v)
	}

	// IsLeaf
	if err := binary.Read(buf, binary.LittleEndian, &node.IsLeaf); err != nil {
		return err
	}

	return nil
}

func (t *Tree)flushNode(n *Node) error {
	if n == nil {
		return fmt.Errorf("flushNode == nil")
	}
	if t.File == nil {
		return fmt.Errorf("flush node into disk, but not open file")
	}

	var (
		length int
		err error
	)

	bs := bytes.NewBuffer(make([]byte, 0))

	// IsActive
	if err = binary.Write(bs, binary.LittleEndian, n.IsDiskInTree); err != nil {
		return nil
	}

	// Children
	childCount := uint8(len(n.Children))
	if err = binary.Write(bs, binary.LittleEndian, childCount); err != nil {
		return err
	}
	for _, v := range n.Children {
		if err = binary.Write(bs, binary.LittleEndian, uint64(v)); err != nil {
			return err
		}
	}

	// Self
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Self)); err != nil {
		return err
	}

	// Next
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Next)); err != nil {
		return err
	}

	// Prev
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Prev)); err != nil {
		return err
	}

	// Parent
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Parent)); err != nil {
		return err
	}

	// Keys
	keysCount := uint8(len(n.Keys))
	if err = binary.Write(bs, binary.LittleEndian, keysCount); err != nil {
		return err
	}
	for _, v := range n.Keys {
		if err= binary.Write(bs,binary.LittleEndian,uint8(len([]byte(v))));err!=nil{
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	// Record
	recordCount := uint8(len(n.Records))
	if err = binary.Write(bs, binary.LittleEndian, recordCount); err != nil {
		return err
	}
	for _, v := range n.Records {
		if err = binary.Write(bs, binary.LittleEndian, uint8(len([]byte(v)))); err != nil {
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	// IsLeaf
	if err = binary.Write(bs, binary.LittleEndian, n.IsLeaf); err != nil {
		return err
	}

	dataLen := len(bs.Bytes())
	if uint64(dataLen) + 8 > t.BlockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen) + 4, t.BlockSize)
	}
	tmpbs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(tmpbs, binary.LittleEndian, uint64(dataLen)); err != nil {
		return err
	}

	data := append(tmpbs.Bytes(), bs.Bytes()...)
	if length, err = t.File.WriteAt(data, int64(n.Self)); err != nil {
		return err
	} else if len(data) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(n.Self), t.File.Name(), len(data), length)
	}
	return nil
}




func (t *Tree)PrintInfo(){
	println("offset: ",t.OffSet)
	println("free blocks len:",len(t.FreeBlocks),"cap: ",cap(t.FreeBlocks))
	/*for _,x:=range t.FreeBlocks {
		print("---",x)
	}*/
	println("block size: ",t.BlockSize)
	println("file size: ",t.FileSize)
	info,_:=t.File.Stat()

	println("file info: ",info.Size(),info.IsDir(),info.Mode(),info.Name(),info.Sys())

	node:=t.NodePool.Get().(*Node)
	println("node self:",node.Self)
	println("node is leaf:",node.IsLeaf)
	println("node records:",node.Records)
	for _,x:=range node.Records{
		println("record:",x)
	}
	println("node keys:",node.Keys)
	for _,x:=range node.Keys{
		println("keys:",x)
	}
	println("node is disk in tree",node.IsDiskInTree)
	println("node parent",node.Parent)
	println("node prev:",node.Prev)
	println("node next:",node.Next)
	println("node children:",node.Children)


}

func (t *Tree)flushAndPushNodePool(n *Node)error{
	if err:=t.flushNode(n);err!=nil{
		return err
	}
	t.pushNodePool(n)
	return nil
}

func (t *Tree)pushNodePool(n *Node){
	t.NodePool.Put(n)
}

func (t *Tree) initNodeForUsage(node *Node) {
	node.IsDiskInTree = true
	node.Children = nil
	node.Self = NIL_OFFSET
	node.Next = NIL_OFFSET
	node.Prev = NIL_OFFSET
	node.Parent = NIL_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree) clearNodeForUsage(node *Node) {
	node.IsDiskInTree = false
	node.Children = nil
	node.Self = NIL_OFFSET
	node.Next = NIL_OFFSET
	node.Prev = NIL_OFFSET
	node.Parent = NIL_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree) InsertToLeaf(key string, val string) error {
	return nil
}






