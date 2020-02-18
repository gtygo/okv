package bplustree

import (
	"fmt"
	"os"
	"testing"
)

type insertTests struct{
	key string
	val string
	wantErr error
}


func TestTree_Insert_No_DBFile(t *testing.T) {
	tests:= []insertTests{
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
	}
	for _,x:=range tests{
		tree,_:=NewTree("test_no_file.db")
		err:=tree.Insert(x.key,x.val)
		if x.wantErr!=err{
			t.Fatalf("test filed got :%s want :%s",err,x.wantErr)
		}
		//todo: 检查kv 插入的正确性


		os.Remove("test_no_file.db")
	}
}

/*
test for db file exist,start a nil offset with tree.file
 */
func TestTree_Insert_Has_DBFile(t *testing.T) {
	tree,_:=NewTree("test_file.db")
	tree.Insert("key1","val1")

	v,_:=tree.Find("key1")

	fmt.Println(v)
	tree.Close()

	tests:= []insertTests{
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
			"testkey3",
			nil,
		},
	}
	fmt.Println("-------------------start test loop-----------------------")
	for _,x:=range tests{
		tree,_:=NewTree("test_file.db")
		err:=tree.Insert(x.key,x.val)
		if x.wantErr!=err{
			t.Fatalf("test filed got :%s want :%s",err,x.wantErr)
		}
		//todo: 检查kv 插入的正确性
		tree.Close()
	}
	os.Remove("test_file.db")
}

func TestTree_Insert_ToLeaf(t *testing.T) {
	tree,_:=NewTree("test_file.db")
	tree.Insert("key1","val1")

	tests:= []insertTests{
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
	for _,x:=range tests{
		err:=tree.Insert(x.key,x.val)
		if x.wantErr!=err{
			t.Fatalf("test filed got :%s want :%s",err,x.wantErr)
		}
	}
	os.Remove("test_file.db")
}

func TestTree_Insert_Update_ParentKey(t *testing.T) {
	os.Remove("test_file.db")
	tree,_:=NewTree("test_file.db")

	tests:= []insertTests{
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
	}

	fmt.Println("-------------------start test loop-----------------------")
	for _,x:=range tests{
		fmt.Println("插入： ",x.key,x.val)
		err:=tree.Insert(x.key,x.val)
		if x.wantErr!=err{
			t.Fatalf("test filed got :%s want :%s",err,x.wantErr)
		}

	}
	tree.PrintWholeTree()

	os.Remove("test_file.db")
}



