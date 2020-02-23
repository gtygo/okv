package bplustree

import (
	"errors"
	"fmt"
	"io"
)

type Node struct {
	IsDiskInTree bool
	Children     []uint64
	Self         uint64
	Next         uint64
	Prev         uint64
	Parent       uint64
	Keys         []string
	Records      []string
	IsLeaf       bool
}

func (t *Tree) initNode(node *Node) {
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

func (t *Tree) clearNode(node *Node) {
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

func (t *Tree) newNodeFromDisk() (*Node, error) {

	node := t.NodePool.Get().(*Node)
	if len(t.FreeBlocks) > 0 {
		off := t.FreeBlocks[0]
		t.FreeBlocks = t.FreeBlocks[1:len(t.FreeBlocks)]
		t.initNode(node)
		node.Self = off
		return node, nil
	}
	if err := t.allocBlock(); err != nil {
		return nil, err
	}
	if len(t.FreeBlocks) > 0 {
		off := t.FreeBlocks[0]
		t.FreeBlocks = t.FreeBlocks[1:len(t.FreeBlocks)]
		t.initNode(node)
		node.Self = off
		return node, nil
	}
	return nil, errors.New("alloc node ")
}

//为tree分配free blocks
//read系统调用获取node 文件的index存入free blocks（非页节点）
func (t *Tree) allocBlock() error {
	node := &Node{}

	bs := t.BlockSize
	for i := uint64(9); i < t.FileSize && len(t.FreeBlocks) < MAX_FREEBLOCK; i += bs {

		if i+bs > t.FileSize {
			break
		}
		if err := t.ReadNode(node, t.File, i); err != nil {
			return err
		}

		if !node.IsLeaf {
			t.FreeBlocks = append(t.FreeBlocks, i)
		}
	}
	nextFile := ((t.FileSize + 4095) / 4096) * 4096

	for len(t.FreeBlocks) < MAX_FREEBLOCK {
		t.FreeBlocks = append(t.FreeBlocks, nextFile)
		nextFile += bs
	}
	t.FileSize = nextFile
	return nil
}

//将各个node写回对象池并放入未提交切片中
func (t *Tree) flushAndPushNodePool(n ...*Node) error {
	for _, x := range n {
		t.UnCommitNodes[x.Self] = *x
		t.NodePool.Put(x)
	}
	return nil
}

func (t *Tree) newRootNode(left *Node, right *Node) error {
	var (
		root *Node
		err  error
	)

	if root, err = t.newNodeFromDisk(); err != nil {
		return err
	}
	root.Keys = append(root.Keys, left.Keys[len(left.Keys)-1])
	root.Keys = append(root.Keys, right.Keys[len(right.Keys)-1])
	root.Children = append(root.Children, left.Self)
	root.Children = append(root.Children, right.Self)
	left.Parent = root.Self
	right.Parent = root.Self

	t.OffSet = root.Self
	return t.flushAndPushNodePool(root)
}

func (t *Tree) PrintWholeTree() {
	var n = &Node{
		IsDiskInTree: false,
		Children:     nil,
		Self:         0,
		Next:         0,
		Prev:         0,
		Parent:       0,
		Keys:         nil,
		Records:      nil,
		IsLeaf:       false,
	}

	fmt.Printf("%v \n", t.OffSet)
	count := 0
	for i := uint64(4096); i < t.FileSize; i += t.BlockSize {
		count++
		if err:=t.ReadNode(n, t.File, i);err==io.EOF{
			break
		}
		fmt.Printf("-------%v \n", n)
	}

}
