package main

import (
	"sync"
)

type Tx struct {
	txType int
	sync.RWMutex
}

func Begin()*Tx{
	return &Tx{
		txType:  0,
		RWMutex: sync.RWMutex{},
	}
}

//数据库中提交操作 对应底层为将所有更改的node进行刷盘操作，如果未发生问题，删除swp.db文件
func (tx *Tx)Commit()error{
	//1. 写入tmp文件，全部写入完成后，会将tmp文件更名为swp.db

	//2. 写入修改完成的node到真正的数据库文件my.db中

	//3. 删除swp.db
	return nil
}


/*
捕获到任何错误均进行回滚
1. commit()以前的错误，直接清理掉node

2. commit过程中写入tmp文件出现的错误，返回

3. 写入修改完成的node时出现错误，使用swp.db来回滚数据库，并删除swp.db

 */
func (tx *Tx)RollBack(){



}

func (tx *Tx)Set(k string ,v string)error{
	return nil
}

func (tx *Tx)Get(k string)(string,error){
	return "nil",nil
}

func (tx *Tx)Delete(k string)error{
	return nil
}

func (tx *Tx)Update(k string,v string)error{
	return nil
}


func main(){

	tx:=Begin()
	if err:=tx.Set("a","b");err!=nil{
		tx.RollBack()
	}
	if err:=tx.Delete("a");err!=nil{
		tx.RollBack()
	}
	tx.Commit()


}