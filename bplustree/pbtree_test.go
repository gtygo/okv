package bplustree

import (
	"fmt"
	"testing"
)

func TestNewTree(t *testing.T) {
	s, _ := NewTree()
	//s.PrintInfo()

	err := s.Insert("key1", "value1")
	if err != nil {
		t.Log(err)
	}
	println("========= insert done 1 =========")
	//s.PrintInfo()

	err = s.Insert("key2", "value2")
	if err != nil {
		t.Log(err)
	}
	println("------------insert done 2-------------------")
	//s.PrintInfo()

	a, err := s.Find("key1")

	fmt.Printf("anwser: %s ,%s \n", a, err)
	b, err := s.Find("key2")
	fmt.Printf("anwser: %s ,%s \n", b, err)

	//update
	s.Update("key1", "xxxxxx----xxxxxx")
	x, err := s.Find("key1")
	fmt.Printf("anwser: %s ,%s \n", x, err)

	err = s.Delete("key1")
	if err != nil {
		t.Fatal(err)
	}

	println("==========delete done============")

	x, err = s.Find("key1")
	fmt.Printf("anwser: %s ,%s \n", x, err)

}

