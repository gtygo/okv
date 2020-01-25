package bptree

import (
	"bytes"
	"encoding/binary"
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

	leaf,err:=t.newMappingNodeFromPool(INVALID_OFFSET)
	if err!=nil{
		return err
	}
	err=t.findLeaf(leaf,key)
	if err!=nil{
		return err
	}
	index,err:=insertIntoLeaf(leaf,key,val)
	if err!=nil{
		return err
	}
	err=t.updateLastParentIdx(leaf,index)
	if err!=nil{
		return err
	}


	if len(leaf.Keys)<=4{
		return t.flushNodeAndAddPool(leaf)
	}

	newNode,err:=t.newNodeFromDisk()
	if err!=nil{
		return err
	}
	newNode.IsLeaf=true

	err=t.splitLeaf(leaf,newNode)
	if err!=nil{
		return err
	}

	err=t.flushNodeAndAddPool(newNode,leaf)
	if err!=nil{
		return err
	}
	return t.insertIntoParent(leaf.Parent,leaf.Self,leaf.Keys[len(leaf.Keys)-1],newNode.Self)
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
	node:=t.nodePool.Get().(*Node)
	t.initNodeForUsage(node)
	if off==INVALID_OFFSET{
		return node,nil
	}
	t.clearNodeForUsage(node)

	err:=t.seekNode(node,off)
	if err!=nil{
		return nil,err
	}
	return node,nil
}

func (t *Tree) seekNode(node *Node, off OffType) error {

	t.clearNodeForUsage(node)


	buf:=make([]byte,8)

	n,err:=t.file.ReadAt(buf,int64(off))
	if err!=nil{
		return err
	}
	if n!=8{
		return errors.New("len error")
	}

	bufBytes:=bytes.NewBuffer(buf)
	dataLen:=uint64(0)
	err=binary.Read(bufBytes,binary.LittleEndian,&dataLen)
	if err!=nil{
		return err
	}
	if dataLen+8>t.blockSize{
		return errors.New("data length too long")
	}

	buf=make([]byte,dataLen)

	n,err=t.file.ReadAt(buf,int64(off)+8)
	if err!=nil{
		return err
	}
	if uint64(n)!=dataLen{
		return errors.New("len error")
	}

	bs:=bytes.NewBuffer(buf)


	err=binary.Read(bs,binary.LittleEndian,&node.IsActive)
	if err!=nil{
		return err
	}

	// Children
	childCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &childCount); err != nil {
		return err
	}
	node.Children = make([]OffType, childCount)
	for i := uint8(0);i < childCount;i++ {
		child := uint64(0)
		if err = binary.Read(bs, binary.LittleEndian, &child); err != nil {
			return err
		}
		node.Children[i] = OffType(child)
	}

	// Self
	self := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &self); err != nil {
		return err
	}
	node.Self = OffType(self)

	// Next
	next := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &next); err != nil {
		return err
	}
	node.Next = OffType(next)

	// Prev
	prev := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &prev); err != nil {
		return err
	}
	node.Prev = OffType(prev)

	// Parent
	parent := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &parent); err != nil {
		return err
	}
	node.Parent = OffType(parent)

	// Keys
	keysCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &keysCount); err != nil {
		return err
	}
	node.Keys = make([]uint64, keysCount)
	for i := uint8(0); i < keysCount;i++ {
		if err = binary.Read(bs, binary.LittleEndian, &node.Keys[i]); err != nil {
			return err
		}
	}

	// Records
	recordCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &recordCount); err != nil {
		return err
	}
	node.Records = make([]string, recordCount)
	for i := uint8(0); i < recordCount;i++ {
		l := uint8(0)
		if err = binary.Read(bs, binary.LittleEndian, &l); err != nil {
			return err
		}
		v := make([]byte, l)
		if err = binary.Read(bs, binary.LittleEndian, &v); err != nil {
			return err
		}
		node.Records[i] = string(v)
	}

	// IsLeaf
	if err = binary.Read(bs, binary.LittleEndian, &node.IsLeaf); err != nil {
		return err
	}
	return nil
}

func (t *Tree) reStructRootNode() error {
	return nil
}

func (t *Tree) checkDiskBlock() error {
	return nil
}

func (t *Tree)flushNodeAndAddPool(node... *Node)error{
	return nil
}

func (t *Tree)updateLastParentIdx(n *Node,idx int)error{
	return nil
}

func insertIntoLeaf(n *Node,key uint64,val string)(int,error){
	return 0,nil
}

func (t *Tree)newNodeFromDisk()(*Node,error){
	return nil,nil
}

func (t *Tree) splitLeaf(leaf *Node, node *Node) error {
	return nil
}

func (t *Tree) insertIntoParent(parent OffType, left OffType, key uint64, right OffType) error {
	return nil
}

func (t *Tree) initNodeForUsage(node *Node) {
	node.IsActive = true
	node.Children = nil
	node.Self = INVALID_OFFSET
	node.Next = INVALID_OFFSET
	node.Prev = INVALID_OFFSET
	node.Parent = INVALID_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree) clearNodeForUsage(node *Node) {
	node.IsActive = false
	node.Children = nil
	node.Self = INVALID_OFFSET
	node.Next = INVALID_OFFSET
	node.Prev = INVALID_OFFSET
	node.Parent = INVALID_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}