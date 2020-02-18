package bplustree

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
)

const (
	NIL_OFFSET    = 0xdeadbeef
	MAX_FREEBLOCK = 500
	ORDER         = 5
)

type Tree struct {
	OffSet     uint64
	NodePool   *sync.Pool
	FreeBlocks []uint64
	File       *os.File
	BlockSize  uint64
	FileSize   uint64
}

func NewTree(name string) (*Tree, error) {

	var fstat os.FileInfo

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	var stat syscall.Statfs_t
	if err = syscall.Statfs(name, &stat); err != nil {
		return nil, err
	}
	blockSize := uint64(stat.Bsize)

	if fstat, err = f.Stat(); err != nil {
		return nil, err
	}

	return &Tree{
		OffSet: NIL_OFFSET,
		NodePool: &sync.Pool{New: func() interface{} {
			return &Node{}
		}},
		FreeBlocks: make([]uint64, 0, MAX_FREEBLOCK),
		File:       f,
		BlockSize:  blockSize,
		FileSize:   uint64(fstat.Size()),
	}, nil
}

func (t *Tree) Close() error {
	if t.File != nil {
		t.File.Sync()
		return t.File.Close()
	}
	return nil
}

func (t *Tree) Insert(key string, val string) error {
	fmt.Println("此时的根节点地址为： ",t.OffSet)
	if t.OffSet == NIL_OFFSET {

		node, err := t.newNodeFromDisk()
		if err != nil {
			return err
		}
		t.OffSet = node.Self
		node.IsDiskInTree = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, val)
		node.IsLeaf = true
		return t.flushAndPushNodePool(node)
	}
	return  t.insertToLeaf(key, val)
}

func (t *Tree) Find(key string) (string, error) {

	if t.OffSet == NIL_OFFSET {
		return "", nil
	}
	node, err := t.newMappingNodeFromPool(NIL_OFFSET)
	if err != nil {
		return "", err
	}

	if err := t.findLeaf(node, key); err != nil {
		fmt.Println("error: ", err)
		return "", err
	}
	defer t.pushNodePool(node)

	for i, x := range node.Keys {
		if x == key {
			return node.Records[i], nil
		}
	}
	return "", errors.New("key not found")
}

func (t *Tree) Update(key string, value string) error {
	if t.OffSet == NIL_OFFSET {
		return errors.New("key not found")
	}

	node, err := t.newMappingNodeFromPool(NIL_OFFSET)
	if err != nil {
		return err
	}

	if err := t.findLeaf(node, key); err != nil {
		return err
	}

	for i, x := range node.Keys {
		if x == key {
			node.Records[i] = value
			return t.flushAndPushNodePool(node)
		}
	}
	return errors.New("key not found")
}

func (t *Tree) Delete(key string) error {
	if t.OffSet == NIL_OFFSET {
		return errors.New("key not found")
	}
	return t.deleteKeyFromLeaf(key)
}

func (t *Tree) PrintInfo() {
	println("offset: ", t.OffSet)
	println("free blocks len:", len(t.FreeBlocks), "cap: ", cap(t.FreeBlocks))
	/*for _,x:=range t.FreeBlocks {
		print("---",x)
	}*/
	println("block size: ", t.BlockSize)
	println("file size: ", t.FileSize)
	info, _ := t.File.Stat()

	println("file info: ", info.Size(), info.IsDir(), info.Mode(), info.Name(), info.Sys())

	node := t.NodePool.Get().(*Node)
	println("node self:", node.Self)
	println("node is leaf:", node.IsLeaf)
	println("node records:", node.Records)
	for _, x := range node.Records {
		println("record:", x)
	}
	println("node keys:", node.Keys)
	for _, x := range node.Keys {
		println("keys:", x)
	}
	println("node is disk in tree", node.IsDiskInTree)
	println("node parent", node.Parent)
	println("node prev:", node.Prev)
	println("node next:", node.Next)
	println("node children:", node.Children)

}




func (t *Tree) insertToLeaf(key string, val string) error {
	println("insert to leaf: ", key, val)
	node, err := t.newMappingNodeFromPool(t.OffSet)
	if err != nil {
		return err
	}

	if err := t.findLeaf(node, key); err != nil {
		return err
	}
	fmt.Printf("插入叶子节点： 从磁盘中获取叶子节点：%v \n",node)
	idx, err := insertKeyValueToLeaf(node, key, val)
	if err != nil {
		return err
	}
	fmt.Printf("将kv插入叶子节点：%v \n",node)
	//update parent key or not
	fmt.Println("更新 父节点")
	if err := t.maybeUpdateParentKey(node, idx); err != nil {
		return err
	}

	if len(node.Keys) <= ORDER {
		fmt.Println("key的数量 不足以发生分裂。。。。")
		return t.flushAndPushNodePool(node)
	}
	fmt.Println("此时需要分裂")
	newNode, err := t.newNodeFromDisk()
	if err != nil {
		return err
	}

	newNode.IsLeaf = true

	if err = t.splitLeaf(node, newNode); err != nil {
		return err
	}

	if err = t.flushAndPushNodePool(newNode, node); err != nil {
		return err
	}

	fmt.Println("node 的父节点： ",node.Parent)
	return  t.insertIntoParent(node.Parent, node.Self, node.Keys[len(node.Keys)-1], newNode.Self)
}

func (t *Tree) newMappingNodeFromPool(off uint64) (*Node, error) {
	node := t.NodePool.Get().(*Node)
	fmt.Printf("从对象池中取出的node： %v \n",node)
	t.initNode(node)

	if off == NIL_OFFSET {
		return node, nil
	}
	t.clearNode(node)
	if err := t.readNode(node, off); err != nil {
		return nil, err
	}
	return node, nil
}

func (t *Tree) findLeaf(node *Node, key string) error {
	offSet := t.OffSet
	if offSet == NIL_OFFSET {
		return nil
	}
	n, err := t.newMappingNodeFromPool(offSet)
	if err != nil {
		return err
	}
	defer t.pushNodePool(n)
	*node = *n
	fmt.Printf("查找 %v \n",node)
	for !node.IsLeaf {
		idx := sort.Search(len(node.Keys), func(i int) bool {
			ans := strings.Compare(key, node.Keys[i])
			return ans == -1 || ans == 0
		})
		fmt.Println("查找叶子节点的索引为：",idx)
		if idx == len(node.Keys) {
			idx = len(node.Keys) - 1
		}
		if err = t.readNode(node, node.Children[idx]); err != nil {
			return err
		}
	}
	return nil
}

func insertKeyValueToLeaf(n *Node, key string, value string) (int, error) {
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return strings.Compare(key, n.Keys[i]) == -1
	})
	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, errors.New("has existed key error")
	}

	n.Keys = append(n.Keys, key)
	n.Records = append(n.Records, value)

	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Records[i] = n.Records[i-1]
	}
	n.Keys[idx] = key
	n.Records[idx] = value
	return idx, nil
}

func insertKeyValIntoNode(n *Node, key string, child uint64) (int, error) {
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return strings.Compare(key, n.Keys[i]) == -1
	})
	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, errors.New("key not found")
	}

	n.Keys = append(n.Keys, key)
	n.Children = append(n.Children, child)
	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Children[i] = n.Children[i-1]
	}
	n.Keys[idx] = key
	n.Children[idx] = child
	return idx, nil
}

func (t *Tree) maybeUpdateParentKey(leaf *Node, idx int) error {
	if idx == len(leaf.Keys)-1 && leaf.Parent != NIL_OFFSET {
		fmt.Println("------需要更新parent key------",idx,leaf.Parent)
		key := leaf.Keys[len(leaf.Keys)-1]
		updateNodeOff := leaf.Parent

		node, err := t.newMappingNodeFromPool(leaf.Self)
		if err != nil {
			return err
		}
		*node = *leaf

		defer t.pushNodePool(node)

		for updateNodeOff != NIL_OFFSET && idx == len(node.Keys)-1 {
			updateNode, err := t.newMappingNodeFromPool(updateNodeOff)
			if err != nil {
				return err
			}

			for i, x := range updateNode.Children {
				if x == node.Self {
					idx = i
					break
				}
			}
			updateNode.Keys[idx] = key

			if err = t.flushAndPushNodePool(updateNode); err != nil {
				return err
			}
			updateNodeOff = updateNode.Parent
			*node = *updateNode
		}
	}

	return nil
}

func (t *Tree) splitLeaf(leaf *Node, node *Node) error {

	var (
		i, split int
	)
	split = cut(ORDER)

	for i = split; i <= ORDER; i++ {
		node.Keys = append(node.Keys, leaf.Keys[i])
		node.Records = append(node.Records, leaf.Records[i])
	}

	// adjust relation
	leaf.Keys = leaf.Keys[:split]
	leaf.Records = leaf.Records[:split]

	node.Next = leaf.Next
	leaf.Next = node.Self
	node.Prev = leaf.Self

	node.Parent = leaf.Parent

	if node.Next != NIL_OFFSET {
		var (
			nextNode *Node
			err      error
		)
		if nextNode, err = t.newMappingNodeFromPool(node.Next); err != nil {
			return err
		}
		nextNode.Prev = node.Self
		if err = t.flushAndPushNodePool(nextNode); err != nil {
			return err
		}
	}

	return nil

}

func (t *Tree) insertIntoParent(parentOff uint64, leftOff uint64, key string, rightOff uint64) error {

	if parentOff == NIL_OFFSET {
		left, err := t.newMappingNodeFromPool(leftOff)
		if err != nil {
			return err
		}
		right, err := t.newMappingNodeFromPool(rightOff)
		if err != nil {
			return err
		}
		if err = t.newRootNode(left, right); err != nil {
			return err
		}
		fmt.Println("111此时根节点地址为：",t.OffSet)
		return t.flushAndPushNodePool(left, right)
	}
	parent, err := t.newMappingNodeFromPool(parentOff)
	if err != nil {
		return err
	}
	idx := getIndex(parent.Keys, key)
	insertToNode(parent, idx, leftOff, key, rightOff)

	if len(parent.Keys) <= ORDER {
		return t.flushAndPushNodePool(parent)
	}
	return t.insertToNodeSplit(parent)

}

func insertToNode(parent *Node, idx int, leftOff uint64, key string, rightOff uint64) {

	parent.Keys = append(parent.Keys, key)

	for i := len(parent.Keys) - 1; i > idx; i-- {
		parent.Keys[i] = parent.Keys[i-1]
	}
	parent.Keys[idx] = key

	if idx == len(parent.Children) {
		parent.Children = append(parent.Children, rightOff)
		return
	}

	tmpChildren := make([]uint64, 30)

	tmpChildren = append(tmpChildren, parent.Children[idx+1:]...)

	parent.Children = append(append(parent.Children[:idx+1], rightOff), tmpChildren...)

}

func getIndex(keys []string, key string) int {
	idx := sort.Search(len(keys), func(i int) bool {
		return strings.Compare(key, keys[i]) == -1
	})
	return idx
}



func (t *Tree) insertToNodeSplit(parent *Node) error {

	var (
		newNode, child, nextNode *Node
		err                      error
		i, split                 int
	)

	if newNode, err = t.newNodeFromDisk(); err != nil {
		return err
	}

	split = cut(ORDER)

	for i = split; i <= ORDER; i++ {
		newNode.Children = append(newNode.Children, parent.Children[i])
		newNode.Keys = append(newNode.Keys, parent.Keys[i])

		// update new_node children relation
		if child, err = t.newMappingNodeFromPool(parent.Children[i]); err != nil {
			return err
		}
		child.Parent = newNode.Self
		if err = t.flushAndPushNodePool(child); err != nil {
			return err
		}
	}
	newNode.Parent = parent.Parent

	parent.Children = parent.Children[:split]
	parent.Keys = parent.Keys[:split]

	newNode.Next = parent.Next
	parent.Next = newNode.Self
	newNode.Prev = parent.Self

	if newNode.Next != NIL_OFFSET {
		if nextNode, err = t.newMappingNodeFromPool(newNode.Next); err != nil {
			return err
		}
		nextNode.Prev = newNode.Self
		if err = t.flushAndPushNodePool(nextNode); err != nil {
			return err
		}
	}

	if err = t.flushAndPushNodePool(parent, newNode); err != nil {
		return err
	}

	return t.insertIntoParent(parent.Parent, parent.Self, parent.Keys[len(parent.Keys)-1], newNode.Self)

}

func (t *Tree) deleteKeyFromLeaf(key string) error {
	leaf, err := t.newMappingNodeFromPool(NIL_OFFSET)
	if err != nil {
		return err
	}

	if err = t.findLeaf(leaf, key); err != nil {
		return err
	}

	println(len(leaf.Keys), cap(leaf.Keys))
	for i, x := range leaf.Keys {
		println(i, x, len(x))
	}

	idx := getIndex(leaf.Keys, key) - 1
	println("idx：", idx)

	if idx == len(leaf.Keys) || leaf.Keys[idx] != key {
		println("leaf keys", leaf.Keys[idx])
		t.pushNodePool(leaf)
		return errors.New("key- not found")
	}

	removeKeyFromLeaf(leaf, idx)

	//if leaf is root

	if leaf.Self == t.OffSet {
		return t.flushAndPushNodePool(leaf)
	}

	if idx == len(leaf.Keys) {
		if err := t.maybeUpdateParentKey(leaf, idx-1); err != nil {
			return err
		}
	}

	if len(leaf.Keys) >= ORDER/2 {
		return t.flushAndPushNodePool(leaf)
	}

	if leaf.Next != NIL_OFFSET {
		nextLeaf, err := t.newMappingNodeFromPool(leaf.Next)
		if err != nil {
			return err
		}
		// lease from next leaf
		if len(nextLeaf.Keys) > ORDER/2 {
			key := nextLeaf.Keys[0]
			rec := nextLeaf.Records[0]
			removeKeyFromLeaf(nextLeaf, 0)
			if idx, err = insertKeyValueToLeaf(leaf, key, rec); err != nil {
				return err
			}

			if err = t.maybeUpdateParentKey(leaf, idx); err != nil {
				return err
			}
			return t.flushAndPushNodePool(nextLeaf, leaf)
		}

		// merge nextLeaf and curleaf
		if leaf.Prev != NIL_OFFSET {
			prevLeaf, err := t.newMappingNodeFromPool(leaf.Prev)
			if err != nil {
				return err
			}
			prevLeaf.Next = nextLeaf.Self
			nextLeaf.Prev = prevLeaf.Self
			if err = t.flushAndPushNodePool(prevLeaf); err != nil {
				return err
			}
		} else {
			nextLeaf.Prev = NIL_OFFSET
		}

		nextLeaf.Keys = append(leaf.Keys, nextLeaf.Keys...)
		nextLeaf.Records = append(leaf.Records, nextLeaf.Records...)

		leaf.IsDiskInTree = false
		t.putFreeBlocks(leaf.Self)

		if err = t.flushAndPushNodePool(leaf, nextLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys)-1])
	}

	// come here because leaf.Next = INVALID_OFFSET
	if leaf.Prev != NIL_OFFSET {
		prevLeaf, err := t.newMappingNodeFromPool(leaf.Prev)
		if err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevLeaf.Keys) > ORDER/2 {
			key := prevLeaf.Keys[len(prevLeaf.Keys)-1]
			rec := prevLeaf.Records[len(prevLeaf.Records)-1]
			removeKeyFromLeaf(prevLeaf, len(prevLeaf.Keys)-1)
			if idx, err = insertKeyValueToLeaf(leaf, key, rec); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.maybeUpdateParentKey(prevLeaf, len(prevLeaf.Keys)-1); err != nil {
				return err
			}
			return t.flushAndPushNodePool(prevLeaf, leaf)
		}
		// merge prevleaf and curleaf
		prevLeaf.Next = NIL_OFFSET
		prevLeaf.Keys = append(prevLeaf.Keys, leaf.Keys...)
		prevLeaf.Records = append(prevLeaf.Records, leaf.Records...)

		leaf.IsDiskInTree = false
		t.putFreeBlocks(leaf.Self)

		if err = t.flushAndPushNodePool(leaf, prevLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys)-2])
	}

	return nil
}

func cut(order int) int {
	return (order + 1) / 2
}

func removeKeyFromLeaf(leaf *Node, idx int) {
	tmpKeys := append([]string{}, leaf.Keys[idx+1:]...)
	leaf.Keys = append(leaf.Keys[:idx], tmpKeys...)

	tmpRecords := append([]string{}, leaf.Records[idx+1:]...)
	leaf.Records = append(leaf.Records[:idx], tmpRecords...)
}

func removeKeyFromNode(node *Node, idx int) {
	tmpKeys := append([]string{}, node.Keys[idx+1:]...)
	node.Keys = append(node.Keys[:idx], tmpKeys...)

	tmpChildren := append([]uint64{}, node.Children[idx+1:]...)
	node.Children = append(node.Children[:idx], tmpChildren...)
}

func (t *Tree) deleteKeyFromNode(off uint64, key string) error {

	if off == NIL_OFFSET {
		return nil
	}
	var (
		node      *Node
		nextNode  *Node
		prevNode  *Node
		newRoot   *Node
		childNode *Node
		idx       int
		err       error
	)
	if node, err = t.newMappingNodeFromPool(off); err != nil {
		return err
	}
	idx = getIndex(node.Keys, key)
	removeKeyFromNode(node, idx)

	// update the last key of parent's if necessary
	if idx == len(node.Keys) {
		if err = t.maybeUpdateParentKey(node, idx-1); err != nil {
			return err
		}
	}

	// if statisfied len
	if len(node.Keys) >= ORDER/2 {
		return t.flushAndPushNodePool(node)
	}

	if off == t.OffSet && len(node.Keys) == 1 {
		if newRoot, err = t.newMappingNodeFromPool(node.Children[0]); err != nil {
			return err
		}
		node.IsDiskInTree = false
		newRoot.Parent = NIL_OFFSET
		t.OffSet = newRoot.Self
		return t.flushAndPushNodePool(node, newRoot)
	}

	if node.Next != NIL_OFFSET {
		if nextNode, err = t.newMappingNodeFromPool(node.Next); err != nil {
			return err
		}
		// lease from next node
		if len(nextNode.Keys) > ORDER/2 {
			key := nextNode.Keys[0]
			child := nextNode.Children[0]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(nextNode, 0)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.maybeUpdateParentKey(node, idx); err != nil {
				return err
			}
			return t.flushAndPushNodePool(node, nextNode, childNode)
		}
		// merge nextNode and curNode
		if node.Prev != NIL_OFFSET {
			if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
				return err
			}
			prevNode.Next = nextNode.Self
			nextNode.Prev = prevNode.Self
			if err = t.flushAndPushNodePool(prevNode); err != nil {
				return err
			}
		} else {
			nextNode.Prev = NIL_OFFSET
		}

		nextNode.Keys = append(node.Keys, nextNode.Keys...)
		nextNode.Children = append(node.Children, nextNode.Children...)

		// update child's parent
		for _, v := range node.Children {
			if childNode, err = t.newMappingNodeFromPool(v); err != nil {
				return err
			}
			childNode.Parent = nextNode.Self
			if err = t.flushAndPushNodePool(childNode); err != nil {
				return err
			}
		}

		node.IsDiskInTree = false
		t.putFreeBlocks(node.Self)

		if err = t.flushAndPushNodePool(node, nextNode); err != nil {
			return err
		}

		// delete parent's key recursively
		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys)-1])
	}

	// come here because node.Next = INVALID_OFFSET
	if node.Prev != NIL_OFFSET {
		if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevNode.Keys) > ORDER/2 {
			key := prevNode.Keys[len(prevNode.Keys)-1]
			child := prevNode.Children[len(prevNode.Children)-1]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(prevNode, len(prevNode.Keys)-1)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.maybeUpdateParentKey(prevNode, len(prevNode.Keys)-1); err != nil {
				return err
			}
			return t.flushAndPushNodePool(prevNode, node, childNode)
		}
		// merge prevNode and curNode
		prevNode.Next = NIL_OFFSET
		prevNode.Keys = append(prevNode.Keys, node.Keys...)
		prevNode.Children = append(prevNode.Children, node.Children...)

		// update child's parent
		for _, v := range node.Children {
			if childNode, err = t.newMappingNodeFromPool(v); err != nil {
				return err
			}
			childNode.Parent = prevNode.Self
			if err = t.flushAndPushNodePool(childNode); err != nil {
				return err
			}
		}

		node.IsDiskInTree = false
		t.putFreeBlocks(node.Self)

		if err = t.flushAndPushNodePool(node, prevNode); err != nil {
			return err
		}

		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys)-2])
	}
	return nil
}

func (t *Tree) putFreeBlocks(off uint64) {
	if len(t.FreeBlocks) >= MAX_FREEBLOCK {
		return
	}
	t.FreeBlocks = append(t.FreeBlocks, off)
}
