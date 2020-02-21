package tx

import (
	"fmt"
	"github.com/gtygo/okv/bplustree"
	"os"
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

	v,err:=tx.Get("d1")
	fmt.Println("hahhahah",v)


	tree.PrintWholeTree()
}


func TestTx_Commit_Second(t *testing.T) {
	tree, err := bplustree.NewTree("my.db")
	if err != nil {
		fmt.Println(err)
		return
	}


	tree.PrintWholeTree()

}

func TestTx_Commit_Cover(t *testing.T){
	fmt.Println("----------------")
	tree, _ := bplustree.NewTree("my.db")
	defer tree.Close()

	f, _ := os.OpenFile("swp.db", os.O_CREATE|os.O_RDWR, 0644)

	tree.CoverSwpFile(f)
	fmt.Println("写入到正式的文件中完成")

	tree.PrintWholeTree()


	v,err:=tree.Find("a8")
	if err!=nil{
		fmt.Println(err)
	}
	fmt.Println("查找a8",v)

	v2,err:=tree.Find("b")
	if err!=nil{
		fmt.Println(err)
	}
	fmt.Println("查找b",v2)

}
