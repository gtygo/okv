package bplustree

import (
	"errors"
	"fmt"
	"io"
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
	OffSet        uint64
	NodePool      *sync.Pool
	FreeBlocks    []uint64
	File          *os.File
	CoverFile     *os.File
	BlockSize     uint64
	FileSize      uint64
	UnCommitNodes map[uint64]Node //key : offset val ：node pointer
}

func NewTree(name string) (*Tree, error) {

	var fstat os.FileInfo

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	root, err := readRootOffset(f)
	if err != nil && err != io.EOF {
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

	fileSize := uint64(fstat.Size())
	if fileSize == 0 {
		fileSize = 8
	}

	tree := &Tree{
		OffSet: root,
		NodePool: &sync.Pool{New: func() interface{} {
			return &Node{}
		}},
		FreeBlocks:    make([]uint64, 0, MAX_FREEBLOCK),
		File:          f,
		BlockSize:     blockSize,
		FileSize:      fileSize,
		UnCommitNodes: make(map[uint64]Node, 10),
	}

	swp, err := os.OpenFile("swp.db", os.O_RDWR, 0644)
	if err == nil {
		//cover
		if err := tree.coverSwpFile(swp); err != nil {
			return nil, err
		}
		fmt.Println("恢复数据成功")
		swp.Close()
		os.Remove("swp.db")
	}
	return tree, nil
}

func (t *Tree) Close() error {
	if t.File != nil {
		t.File.Sync()
		return t.File.Close()
	}
	return nil
}

func (t *Tree) Insert(key string, val string) error {
	fmt.Println("此时的根节点地址为： ", t.OffSet)
	if t.OffSet == NIL_OFFSET {
		fmt.Println("第一次插入")
		node, err := t.newNodeFromDisk()
		if err != nil {
			return err
		}
		t.OffSet = node.Self
		node.IsDiskInTree = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, val)
		node.IsLeaf = true

		fmt.Printf("未放入的node: %v \n", node)
		t.flushAndPushNodePool(node)
		return nil
	}
	return t.insertToLeaf(key, val)
}

func (t *Tree) Find(key string) (string, error) {
	if t.OffSet == NIL_OFFSET {
		return "", nil
	}
	node, err := t.getNodeByCacheOrDisk(t.OffSet)
	if err != nil {
		return "", err
	}

	if err := t.findLeaf(node, key); err != nil {
		fmt.Println("error: ", err)
		return "", err
	}
	defer t.NodePool.Put(node)

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

	node, err := t.getNodeByCacheOrDisk(NIL_OFFSET)
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

func (t *Tree) insertToLeaf(key string, val string) error {
	println("insert to leaf: ", key, val)
	node, err := t.getNodeByCacheOrDisk(t.OffSet)
	if err != nil {
		return err
	}

	if err := t.findLeaf(node, key); err != nil {
		return err
	}
	fmt.Printf("插入叶子节点： 从磁盘中获取叶子节点：%v \n", node)
	idx, err := insertKeyValueToLeaf(node, key, val)
	if err != nil {
		return err
	}
	fmt.Printf("将kv插入叶子节点：%v \n", node)
	//update parent key or not
	fmt.Println("更新 父节点")
	if err := t.maybeUpdateParentKey(node, idx); err != nil {
		return err
	}

	if len(node.Keys) <= ORDER {
		fmt.Println("key的数量 不足以发生分裂。。。。")
		return t.flushAndPushNodePool(node)
	}
	fmt.Println("此时需要分裂-------------")
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

	fmt.Println("node 的父节点： ", node.Parent)
	return t.insertIntoParent(node.Parent, node.Self, node.Keys[len(node.Keys)-1], newNode.Self)
}

func (t *Tree) getNodeByCacheOrDisk(off uint64) (*Node, error) {
	fmt.Println("从磁盘中读取off 对应的node", off)

	node := t.NodePool.Get().(*Node)
	t.initNode(node)

	if off == NIL_OFFSET {
		return node, nil
	}
	t.clearNode(node)
	//先读取tree中缓存的未提交node，如果没有对应off则向磁盘中读取
	if v, ok := t.UnCommitNodes[off]; ok {
		node = &v
	} else {
		//缓存未命中，向磁盘中进行查找
		if err := t.readNode(node, t.File, off); err != nil {
			return nil, err
		}
	}
	fmt.Printf("在map中找到了对应的node：%v \n", node)
	return node, nil
}

func (t *Tree) findLeaf(node *Node, key string) error {
	offSet := t.OffSet
	if offSet == NIL_OFFSET {
		return nil
	}
	n, err := t.getNodeByCacheOrDisk(offSet)
	if err != nil {
		return err
	}
	defer t.NodePool.Put(n)

	fmt.Printf("查找 %v \n", node)
	for !n.IsLeaf {
		fmt.Printf("还不是叶子节点 %v \n", n)
		idx := sort.Search(len(n.Keys), func(i int) bool {
			ans := strings.Compare(key, n.Keys[i])
			return ans == -1 || ans == 0
		})
		fmt.Println("查找叶子节点的索引为：", idx)
		if idx == len(n.Keys) {
			idx = len(n.Keys) - 1
		}
		n, err = t.getNodeByCacheOrDisk(n.Children[idx])
		fmt.Printf("此时换出的节点为叶子节点: %v \n", n)
		if err != nil {
			return err
		}
	}

	*node = *n
	fmt.Printf("查找叶子节点结束: %v \n", node)
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
		fmt.Println("------需要更新parent key------", idx, leaf.Parent)
		key := leaf.Keys[len(leaf.Keys)-1]
		updateNodeOff := leaf.Parent

		node, err := t.getNodeByCacheOrDisk(leaf.Self)
		if err != nil {
			return err
		}
		*node = *leaf

		defer t.NodePool.Put(node)

		for updateNodeOff != NIL_OFFSET && idx == len(node.Keys)-1 {
			updateNode, err := t.getNodeByCacheOrDisk(updateNodeOff)
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

func (t *Tree) splitLeaf(node *Node, newNode *Node) error {

	var (
		i, split int
	)
	split = cut(ORDER)

	for i = split; i <= ORDER; i++ {
		newNode.Keys = append(newNode.Keys, node.Keys[i])
		newNode.Records = append(newNode.Records, node.Records[i])
	}

	node.Keys = node.Keys[:split]
	node.Records = node.Records[:split]

	newNode.Next = node.Next
	node.Next = newNode.Self
	newNode.Prev = node.Self

	newNode.Parent = node.Parent
	/*
		if newNode.Next != NIL_OFFSET {
			fmt.Println("此时next指向为空")
			var (
				nextNode *Node
				err      error
			)
			//todo: 这里不需要重新从磁盘获取node 这里的系统调用read write是多余的
			if nextNode, err = t.getNodeByCacheOrDisk(newNode.Next); err != nil {
				return err
			}
			nextNode.Prev = newNode.Self
			if err = t.flushAndPushNodePool(nextNode); err != nil {
				return err
			}
		}
	*/
	return nil

}

func (t *Tree) insertIntoParent(parentOff uint64, leftOff uint64, key string, rightOff uint64) error {
	fmt.Println("父节点的key需要更新")
	if parentOff == NIL_OFFSET {

		left, err := t.getNodeByCacheOrDisk(leftOff)
		if err != nil {
			return err
		}
		right, err := t.getNodeByCacheOrDisk(rightOff)
		if err != nil {
			return err
		}
		if err = t.newRootNode(left, right); err != nil {
			return err
		}
		fmt.Println("111此时根节点地址为：", t.OffSet)
		return t.flushAndPushNodePool(left, right)
	}
	fmt.Println("node父节点不为空")
	parent, err := t.getNodeByCacheOrDisk(parentOff)
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
		ans := strings.Compare(key, keys[i])
		return ans == -1 || ans == 0
	})
	return idx
}

func (t *Tree) insertToNodeSplit(parent *Node) error {

	var (
		newNode  *Node
		err      error
		i, split int
	)

	if newNode, err = t.newNodeFromDisk(); err != nil {
		return err
	}

	split = cut(ORDER)

	for i = split; i <= ORDER; i++ {
		newNode.Children = append(newNode.Children, parent.Children[i])
		newNode.Keys = append(newNode.Keys, parent.Keys[i])
		if v, ok := t.UnCommitNodes[parent.Children[i]]; ok {
			v.Parent = newNode.Self
			t.UnCommitNodes[parent.Children[i]] = v
			fmt.Println("此时在未提交的node中找到了需要修改的子节点")
		} else {
			child, err := t.getNodeByCacheOrDisk(parent.Children[i])
			if err != nil {
				return err
			}
			child.Parent = newNode.Self
			if err = t.flushAndPushNodePool(child); err != nil {
				return err
			}
		}
	}
	newNode.Parent = parent.Parent

	parent.Children = parent.Children[:split]
	parent.Keys = parent.Keys[:split]

	newNode.Next = parent.Next
	parent.Next = newNode.Self
	newNode.Prev = parent.Self
	/*
		if newNode.Next != NIL_OFFSET {
			if nextNode, err = t.getNodeByCacheOrDisk(newNode.Next); err != nil {
				return err
			}
			nextNode.Prev = newNode.Self
			if err = t.flushAndPushNodePool(nextNode); err != nil {
				return err
			}
		}
	*/
	if err = t.flushAndPushNodePool(parent, newNode); err != nil {
		return err
	}

	return t.insertIntoParent(parent.Parent, parent.Self, parent.Keys[len(parent.Keys)-1], newNode.Self)

}

func (t *Tree) deleteKeyFromLeaf(key string) error {
	fmt.Println("-----开始删除")
	leaf, err := t.getNodeByCacheOrDisk(NIL_OFFSET)
	if err != nil {
		return err
	}

	if err = t.findLeaf(leaf, key); err != nil {
		return err
	}
	fmt.Printf("获取到的叶子节点的node为 %v \n", leaf)
	idx := getIndex(leaf.Keys, key)

	fmt.Println("-----获取叶子节点的index", idx, "此时叶子节点key的长度为：", len(leaf.Keys))

	if idx == len(leaf.Keys) || leaf.Keys[idx] != key {
		t.NodePool.Put(leaf)
		return errors.New("key not found")
	}

	removeKeyFromLeaf(leaf, idx)

	//if leaf is root

	if leaf.Self == t.OffSet {
		fmt.Println("叶子节点为根节点，直接返回")
		return t.flushAndPushNodePool(leaf)
	}

	if idx == len(leaf.Keys) {
		if err := t.maybeUpdateParentKey(leaf, idx-1); err != nil {
			return err
		}
	}

	if len(leaf.Keys) >= ORDER/2 {
		fmt.Println("叶子节点长度大于 m/2 不需要做合并操作", len(leaf.Keys), ORDER/2)
		return t.flushAndPushNodePool(leaf)
	}
	fmt.Println("叶子节点值的数量过少需要进行平衡")
	if leaf.Next != NIL_OFFSET {
		fmt.Println("此时右兄弟节点非空")
		nextLeaf, err := t.getNodeByCacheOrDisk(leaf.Next)
		if err != nil {
			return err
		}
		// lease from next leaf
		if len(nextLeaf.Keys) > ORDER/2 {
			fmt.Println("next leaf的元素数量足够进行平衡，此时借一个后直接返回")
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
		fmt.Println("发现next leaf的元素不够用，这时会合并左右节点")
		// merge nextLeaf and curleaf
		if leaf.Prev != NIL_OFFSET {
			prevLeaf, err := t.getNodeByCacheOrDisk(leaf.Prev)
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
		fmt.Println("此时左兄弟节点非空")
		fmt.Println()
		prevLeaf, err := t.getNodeByCacheOrDisk(leaf.Prev)
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
	if node, err = t.getNodeByCacheOrDisk(off); err != nil {
		return err
	}
	idx = getIndex(node.Keys, key)
	removeKeyFromNode(node, idx)

	if idx == len(node.Keys) {
		if err = t.maybeUpdateParentKey(node, idx-1); err != nil {
			return err
		}
	}

	if len(node.Keys) >= ORDER/2 {
		return t.flushAndPushNodePool(node)
	}

	if off == t.OffSet && len(node.Keys) == 1 {
		if newRoot, err = t.getNodeByCacheOrDisk(node.Children[0]); err != nil {
			return err
		}
		node.IsDiskInTree = false
		newRoot.Parent = NIL_OFFSET
		t.OffSet = newRoot.Self
		return t.flushAndPushNodePool(node, newRoot)
	}

	if node.Next != NIL_OFFSET {
		if nextNode, err = t.getNodeByCacheOrDisk(node.Next); err != nil {
			return err
		}
		// lease from next node
		if len(nextNode.Keys) > ORDER/2 {
			key := nextNode.Keys[0]
			child := nextNode.Children[0]

			// update child's parent
			if childNode, err = t.getNodeByCacheOrDisk(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(nextNode, 0)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}

			if err = t.maybeUpdateParentKey(node, idx); err != nil {
				return err
			}
			return t.flushAndPushNodePool(node, nextNode, childNode)
		}

		if node.Prev != NIL_OFFSET {
			if prevNode, err = t.getNodeByCacheOrDisk(node.Prev); err != nil {
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

		for _, v := range node.Children {
			if childNode, err = t.getNodeByCacheOrDisk(v); err != nil {
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

	if node.Prev != NIL_OFFSET {
		if prevNode, err = t.getNodeByCacheOrDisk(node.Prev); err != nil {
			return err
		}
		if len(prevNode.Keys) > ORDER/2 {
			key := prevNode.Keys[len(prevNode.Keys)-1]
			child := prevNode.Children[len(prevNode.Children)-1]

			if childNode, err = t.getNodeByCacheOrDisk(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(prevNode, len(prevNode.Keys)-1)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}

			if err = t.maybeUpdateParentKey(prevNode, len(prevNode.Keys)-1); err != nil {
				return err
			}
			return t.flushAndPushNodePool(prevNode, node, childNode)
		}

		prevNode.Next = NIL_OFFSET
		prevNode.Keys = append(prevNode.Keys, node.Keys...)
		prevNode.Children = append(prevNode.Children, node.Children...)

		for _, v := range node.Children {
			if childNode, err = t.getNodeByCacheOrDisk(v); err != nil {
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

func (t *Tree) CommitAllNodes(f *os.File, isCoverFile bool) error {
	//更新根节点地址
	if isCoverFile {
		fmt.Println("写入修改到cover file")
		if err := writeRootOffset(f, t.OffSet); err != nil {
			return err
		}
		startOff := int64(4096)
		for _, x := range t.UnCommitNodes {
			if err := t.writeNode(f, &x, true, startOff); err != nil {
				return err
			}
			startOff += 4096
		}
	}

	fmt.Println("非 cover file 此时为正式提交")
	if err := writeRootOffset(f, t.OffSet); err != nil {
		return err
	}
	for _, x := range t.UnCommitNodes {
		fmt.Printf("node: %v \n", x)
		if err := t.writeNode(f, &x, false, 0); err != nil {
			return err
		}
	}
	return nil
}

//读取swp.db文件，并将其恢复到unCommitNode中
func (t *Tree) coverSwpFile(f *os.File) error {
	stat, _ := f.Stat()

	n := t.NodePool.Get().(*Node)
	defer t.NodePool.Put(n)
	t.initNode(n)

	rootOff, err := readRootOffset(f)
	if err != nil {
		return err
	}
	t.OffSet = rootOff
	fmt.Println("根节点地址为：", t.OffSet, stat.Size())
	for i := uint64(4096); i < uint64(stat.Size()); i += t.BlockSize {
		fmt.Println("读node")
		if err := t.readNode(n, f, i); err != nil {
			fmt.Println("err:", err)
			return err
		}
		fmt.Printf("从恢复文件中获取到的node %v \n", n)

		if err := t.writeNode(t.File, n, false, 0); err != nil {
			return err
		}
	}
	return nil
}
