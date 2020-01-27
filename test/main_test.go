package main

import (
	"testing"
)

func TestNewTree(t *testing.T) {
	s, _ := NewTree()
	s.PrintInfo()

	s.Insert("key1", "value1")
	println("========= insert done =========")
	s.PrintInfo()

}

func TestInsert(t *testing.T) {

}
