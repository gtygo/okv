package tx

import (
	"fmt"
	"github.com/gtygo/okv/bplustree"
)

type Tx struct {
	NodePtrCol []*bplustree.Node
	txType     int
	tree       *bplustree.Tree
}

func Begin(t *bplustree.Tree, txType int) *Tx {
	return &Tx{
		tree:   t,
		txType: txType,
	}
}

//数据库中提交操作 对应底层为将所有更改的node进行刷盘操作，如果未发生问题，删除swp.db文件
func (tx *Tx) Commit() error {
	//1. 写入tmp文件，全部写入完成后，会将tmp文件更名为swp.db

	//2. 写入修改完成的node到真正的数据库文件my.db中

	//3. 删除swp.db
	return tx.tree.CommitAllNodes()
}

/*
捕获到任何错误均进行回滚
1. commit()以前的错误，直接清理掉node

2. commit过程中写入tmp文件出现的错误，返回

3. 写入修改完成的node时出现错误，使用swp.db来回滚数据库，并删除swp.db

*/
func (tx *Tx) RollBack() {
	fmt.Println("挂了")

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
