package okv

import "testing"

func TestKV_Set(t *testing.T) {
	err:=set([]byte("123"),[]byte("456"))
	if err!=nil{
		t.Fatal(err)
	}
}
func TestGet(t *testing.T){
	s,err:=get([]byte("123"))
	if err!=nil{
		t.Fatal(err)
	}
	t.Log(s)
}
