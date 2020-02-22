package tx

import (
	"fmt"
	"os"

	"github.com/gtygo/okv/bplustree"
)

const (
	UnCommitMem state = iota
	UnCommitTmp
	UnCommitDb
	CommitDone
)

type state int

type Tx struct {
	txType       int
	tree         *bplustree.Tree
	txState      state
	rollBackItem rollBack
}

type rollBack struct {
	off           uint64
	rollBackNodes map[uint64]bplustree.Node
}

func Begin(t *bplustree.Tree, txType int) *Tx {
	return &Tx{
		tree:    t,
		txType:  txType,
		txState: UnCommitMem,
		rollBackItem: rollBack{
			off:           t.OffSet,
			rollBackNodes: nil,
		},
	}
}

//数据库中提交操作 对应底层为将所有更改的node进行刷盘操作，如果未发生问题，删除swp.db文件
func (tx *Tx) Commit() error {
	tx.txState = UnCommitTmp
	//1. 写入tmp文件，全部写入完成后，会将tmp文件更名为swp.db
	tmp, err := os.OpenFile("tmp", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if err := tx.CommitAllNodes(tmp, true); err != nil {
		return err
	}
	tmp.Close()
	//改名字
	if err := os.Rename("tmp", "swp.db"); err != nil {
		return err
	}
	tx.txState = UnCommitDb
	//2. 写入修改完成的node到真正的数据库文件my.db中
	if err := tx.CommitAllNodes(tx.tree.File, false); err != nil {
		return err
	}
	fmt.Println("事物提交成功")
	//3. 删除swp.db
	os.Remove("swp.db")
	tx.txState = CommitDone
	return nil
}

/*
捕获到任何错误均进行回滚
1. commit()以前的错误，直接清理掉node

2. commit过程中写入tmp文件出现的错误，返回

3. 写入修改完成的node时出现错误，使用swp.db来回滚数据库，并删除swp.db

*/
func (tx *Tx) RollBack() {
	switch tx.txState {

	case UnCommitTmp:
		os.Remove("tmp")
		return
	case UnCommitDb:
		//roll back off
		bplustree.WriteRootOffset(tx.tree.File, tx.rollBackItem.off)

		for _, x := range tx.rollBackItem.rollBackNodes {
			tx.tree.WriteNode(tx.tree.File, &x, false, 0)
		}
		return
	}
}

func (tx *Tx) Set(k string, v string) error {

	//1. 不分裂

	//2. 分裂 分裂过程中会申请分配一个新的node

	return tx.tree.Insert(k, v)
}

func (tx *Tx) Get(k string) (string, error) {
	return tx.tree.Find(k)
}

func (tx *Tx) Delete(k string) error {
	return tx.tree.Delete(k)
}

func (tx *Tx) Update(k string, v string) error {
	return tx.tree.Update(k, v)
}

func (tx *Tx) CommitAllNodes(f *os.File, isCoverFile bool) error {
	if isCoverFile {
		fmt.Println("写入修改到cover file")
		if err := bplustree.WriteRootOffset(f, tx.tree.OffSet); err != nil {
			return err
		}
		startOff := int64(4096)
		for _, x := range tx.tree.UnCommitNodes {
			if err := tx.tree.WriteNode(f, &x, true, startOff); err != nil {
				return err
			}
			startOff += 4096
		}
		return nil
	}

	fmt.Println("非 cover file 此时为正式提交")
	off, err := bplustree.ReadRootOffset(f)
	if err != nil {
		return err
	}
	tx.rollBackItem.off = off
	if err := bplustree.WriteRootOffset(f, tx.tree.OffSet); err != nil {
		return err
	}
	for _, x := range tx.tree.UnCommitNodes {
		fmt.Printf("node: %v \n", x)
		n, _ := tx.tree.NodePool.Get().(*bplustree.Node)
		if err := tx.tree.ReadNode(n, f, x.Self); err != nil {
			return err
		}
		tx.rollBackItem.rollBackNodes[n.Self] = *n
		tx.tree.NodePool.Put(n)
		if err := tx.tree.WriteNode(f, &x, false, 0); err != nil {
			return err
		}
	}
	return nil
}
