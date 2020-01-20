package bptree

import (
	"github.com/pkg/errors"
	"os"
	"sync"
	"syscall"
)

const (
	INVALID_OFFSET = 0xdeadbeef
	MAX_FREEBLOCKS = 100
)

type OffType uint64

type Tree struct {
	rootOff    OffType
	nodePool   *sync.Pool
	freeBlocks []OffType
	file       *os.File
	blockSize  uint64
	fileSize   uint64
}

type Node struct {
	IsActive bool
	Children []OffType
	Self     OffType
	Next     OffType
	Prev     OffType
	Parent   OffType
	Keys     []uint64
	Records  []string
	IsLeaf   bool
}

//NewTree init a  b+ tree
func NewTree(filename string) (*Tree, error) {
	var stat syscall.Statfs_t
	t := &Tree{}
	t.rootOff = INVALID_OFFSET
	t.nodePool = &sync.Pool{
		New: func() interface{} {
			return &Node{}
		},
	}
	t.freeBlocks = make([]OffType, 0, MAX_FREEBLOCKS)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	t.file = f

	if err = syscall.Statfs(filename, &stat); err != nil {
		return nil, err
	}
	t.blockSize = uint64(stat.Bsize)
	if t.blockSize == 0 {
		return nil, errors.New("block size should be zero")
	}
	fstat, err := t.file.Stat()
	if err != nil {
		return nil, err
	}
	t.fileSize = uint64(fstat.Size())
	if t.fileSize != 0 {
		//todo: restruct root node
	}
	return t, nil
}

func (t *Tree) Close() error {
	if t.file != nil {
		t.file.Sync()
		return t.file.Close()
	}
	return nil
}

func (t *Tree) Find(key uint64) (string, error) {
	if t.rootOff==INVALID_OFFSET{
		return "",nil
	}

	node,err:=t.newMappingNodeFromPool(INVALID_OFFSET)
	if err!=nil{
		return "",err
	}
	if err :=t.findLeaf(node,key);err!=nil{
		return "",err
	}
	defer t.putNodePool(node)

	for i,k:=range node.Keys {
		if k==key{
			return node.Records[i],nil
		}
	}
	return "",errors.New("Key Not Found")
}

func (t *Tree) Insert(key uint64, val string) error {
	//section 1 : create node
	if t.rootOff == INVALID_OFFSET {
		node,err:=t.newNode()
		if err!=nil{
			return err
		}
		t.rootOff=node.Self
		node.IsActive=true
		node.Keys=append(node.Keys,key)
		node.Records=append(node.Records,val)
		node.IsLeaf=true
		return t.flushNodeAndAddPool(node)
	}
	//section 2 : insert into leaf node
	return t.insertIntoLeaf(key ,val)
}


func (t *Tree)insertIntoLeaf(key uint64,val string)error{
	return nil
}

func (t *Tree) Update(key uint64, val string) error {
	return nil
}

func (t Tree) Delete(key uint64) error {
	return nil
}

func (t *Tree) PrintTree() error {
	return nil
}


func (t *Tree)findLeaf(node *Node,key uint64)error{
	return nil
}

func (t *Tree) newNode() (*Node, error) {
	return nil,nil
}

func (t Tree)putNodePool(n *Node){
	t.nodePool.Put(n)
}

func (t Tree)newMappingNodeFromPool(off OffType)(*Node ,error){
	return nil,nil
}

func (t *Tree) seekNode(node *Node, off OffType) error {
	return nil
}

func (t *Tree) reStructRootNode() error {
	return nil
}

func (t *Tree) checkDiskBlock() error {
	return nil
}

func (t *Tree)flushNodeAndAddPool(node *Node)error{
	return nil
}