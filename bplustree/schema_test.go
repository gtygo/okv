package bplustree

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func checkDiskNode(t *Tree) []*Node {
	var ans []*Node
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

	count := 0
	for i := uint64(0); i < t.FileSize; i += t.BlockSize {
		if count == 10 {
			break
		}
		count++
		ans = append(ans, n)
	}
	return ans
}

type insertTests struct {
	key     string
	val     string
	wantErr error
}

/*
test no db file

first start open file should be use a normal offset

a ------->b
|         |
value1  value2

*/
func TestTree_Insert_No_DBFile(t *testing.T) {
	tests := []insertTests{
		{
			"a",
			"value1",
			nil,
		},
		{
			"b",
			"value2",
			nil,
		},
	}
	for _, x := range tests {
		tree, _ := NewTree("test_no_file.db")
		err := tree.Insert(x.key, x.val)
		if x.wantErr != err {
			t.Fatalf("test filed got :%s want :%s", err, x.wantErr)
		}
		//todo: 检查kv 插入的正确性

		os.Remove("test_no_file.db")
	}
}

/*
test for db file exist,start a nil offset with tree.file


  a ------->b --------->c
  |         |           |
value1  value2      value3

*/
func TestTree_Insert_Has_DBFile(t *testing.T) {
	tree, _ := NewTree("test_file.db")
	tree.Insert("key1", "val1")

	v, _ := tree.Find("key1")

	fmt.Println(v)
	tree.Close()

	tests := []insertTests{
		{
			"a",
			"value1",
			nil,
		},
		{
			"b",
			"value2",
			nil,
		},
		{
			"c",
			"value3",
			nil,
		},
	}
	fmt.Println("-------------------start test loop-----------------------")
	for _, x := range tests {
		tree, _ := NewTree("test_file.db")
		err := tree.Insert(x.key, x.val)
		if x.wantErr != err {
			t.Fatalf("test filed got :%s want :%s", err, x.wantErr)
		}
		//todo: 检查kv 插入的正确性
		tree.Close()
	}
	os.Remove("test_file.db")
}

func TestTree_Insert_ToLeaf(t *testing.T) {
	tree, _ := NewTree("test_file.db")
	tree.Insert("key1", "val1")

	tests := []insertTests{
		{
			"testkey1",
			"testvalue1",
			nil,
		},
		{
			"testkey2",
			"testvalue2",
			nil,
		},
		{
			"testkey3",
			"testvalue3",
			nil,
		},
	}

	fmt.Println("-------------------start test loop-----------------------")
	for _, x := range tests {
		err := tree.Insert(x.key, x.val)
		if x.wantErr != err {
			t.Fatalf("test filed got :%s want :%s", err, x.wantErr)
		}
	}
	os.Remove("test_file.db")
}

/*
this test will cover leaf node split and update Parent

       c -----------> f ------------>i
       /              |              |
      /               |              |
    a--b--c------>d---e---f------->g---h---i---j
*/
func TestTree_Insert_Update_ParentKey(t *testing.T) {
	os.Remove("test_file.db")
	tree, _ := NewTree("test_file.db")

	tests := []insertTests{
		{
			"a",
			"testvalue1",
			nil,
		},
		{
			"b",
			"testvalue2",
			nil,
		},
		{
			"c",
			"testvalue3",
			nil,
		},
		{
			"d",
			"testvalue3",
			nil,
		},
		{
			"e",
			"testvalue3",
			nil,
		},
		{
			"f",
			"testvalue3",
			nil,
		},

		{
			"g",
			"testvalue3",
			nil,
		},

		{
			"h",
			"testvalue3",
			nil,
		},
		{
			"i",
			"testvalue3",
			nil,
		},
		{
			"j",
			"testvalue3",
			nil,
		},
		{
			"fa",
			"testvalue3",
			nil,
		},
		/*
			{
				"fb",
				"testvalue3",
				nil,
			},
			{
				"fc",
				"testvalue3",
				nil,
			},
		*/
	}

	fmt.Println("-------------------start test loop-----------------------")
	for _, x := range tests {
		fmt.Println("插入： ", x.key, x.val)
		err := tree.Insert(x.key, x.val)
		if x.wantErr != err {
			t.Fatalf("test filed got :%s want :%s", err, x.wantErr)
		}

	}
	tree.PrintWholeTree()

	os.Remove("test_file.db")
}

func TestTree_Delete(t *testing.T) {
	os.Remove("test_del.db")
	tree, _ := NewTree("test_del.db")
	defer tree.Close()

	for i := 0; i < 6; i++ {
		tree.Insert(strconv.Itoa(i), "val")
	}
	err := tree.Delete("2")
	if err != nil {
		fmt.Printf("删除失败 %v \n", err)
	} else {
		fmt.Println("删除成功")
	}

	err = tree.Delete("1")
	if err != nil {
		fmt.Printf("删除失败 %v \n", err)
	} else {
		fmt.Println("删除成功")
	}
	err = tree.Delete("3")
	if err != nil {
		fmt.Printf("删除失败 %v \n", err)
	} else {
		fmt.Println("删除成功")
	}

	tree.PrintWholeTree()

	os.Remove("test_del.db")
}

func TestTree_ReadTx(t *testing.T) {
	tree, _ := NewTree("my.db")

	tree.Close()
}
