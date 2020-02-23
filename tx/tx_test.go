package tx

import (
	"fmt"
	"github.com/gtygo/okv/bplustree"
	"os"
	"strconv"
	"testing"
)


func TestTx_Commit_First(t *testing.T) {
	tree, err := bplustree.NewTree("my.db")
	if err != nil {
		fmt.Println(err)
		return
	}

	tx := Begin(tree, 0)
	if err := tx.Set("a", "bbbb"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("b", "b"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("a1", "b"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("a2", "b"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("a3", "b"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("a4", "b"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("a5", "b"); err != nil {
		fmt.Println(err)
		tx.RollBack()
		return
	}

	if err := tx.Set("a6", "b"); err != nil {
		tx.RollBack()
		return
	}

	if err := tx.Set("a7", "b"); err != nil {
		tx.RollBack()
		return
	}

	if err := tx.Set("a8", "b"); err != nil {
		tx.RollBack()
		return
	}
	if err := tx.Set("b1", "b"); err != nil {
		tx.RollBack()
		return
	}

	if err := tx.Set("b2", "b"); err != nil {
		tx.RollBack()
		return
	}

	if err := tx.Set("b3", "b"); err != nil {
		tx.RollBack()
		return
	}
	if err := tx.Set("c", "b"); err != nil {
		tx.RollBack()
		return
	}
	if err := tx.Set("c1", "b"); err != nil {
		tx.RollBack()
		return
	}
	if err := tx.Set("c2", "b"); err != nil {
		tx.RollBack()
		return
	}

	if err := tx.Set("d", "b"); err != nil {
		tx.RollBack()
		return
	}

	if err := tx.Set("d1", "d1的值为000x9102"); err != nil {
		tx.RollBack()
		return
	}

	/*
		if err := tx.Set("d2", "b"); err != nil {
			tx.RollBack()
			return
		}
	*/

	if err := tx.Commit(); err != nil {
		tx.RollBack()
		return
	}
	fmt.Println("commit  完成")

	v, err := tx.Get("d1")
	fmt.Println("hahhahah", v)

	tree.PrintWholeTree()
}


/*

第一次测试：
阶树置为 20
	插入10万条kv 花费 2.718s
	插入100万条kv 花费 35.492s

尝试提高阶树到 40
	插入10万条kv  花费 1.346s
	插入100万条 kv 花费 18.160s

提高阶树到 80
	插入10万条kv 花费 0.696s
	插入100万条kv 花费 9.849s

 */
func TestTx_Insert(t *testing.T) {
	os.Remove("my.db")
	os.Remove("swp.db")
	caseNum:=1000000
	tree, _ := bplustree.NewTree("my.db")

	tx:=Begin(tree,0)
	for i:=0;i<=caseNum;i++{
		k:=strconv.Itoa(i)
		v:=strconv.Itoa(i)
		if err:=tx.Set(k,v);err!=nil{
			t.Errorf("put failed %v,%v",err,i)
			os.Remove("my.db")
			os.Remove("swp.db")
			return
		}
	}
	fmt.Println("准备commit")
	if err:=tx.Commit();err!=nil{
		t.Errorf("commit failed %v",err)
		os.Remove("my.db")
		os.Remove("swp.db")
		return
	}
	fmt.Println("commit完成")
	//tree.PrintWholeTree()

	os.Remove("my.db")
	os.Remove("swp.db")

}

func TestTx_Delete(t *testing.T) {

}

func TestTx_Find(t *testing.T) {

}

