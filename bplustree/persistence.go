package bplustree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)
//磁盘中的node映射到内存中（read系统调用+binary解码）赋值到node中
func (t *Tree) readNode(node *Node, off uint64) error {
	t.clearNode(node)
	b := make([]byte, 8)
	if _, err := t.File.ReadAt(b, int64(off)); err != nil {
		return err
	}

	buf := bytes.NewBuffer(b)

	var dataLen uint64

	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return err
	}

	b = make([]byte, dataLen)
	if _, err := t.File.ReadAt(b, int64(off)+8); err != nil {
		return err
	}

	buf = bytes.NewBuffer(b)

	//is disk in tree
	if err := binary.Read(buf, binary.LittleEndian, &node.IsDiskInTree); err != nil {
		return err
	}

	//children count
	childCount := uint8(0)
	if err := binary.Read(buf, binary.LittleEndian, &childCount); err != nil {
		return err
	}

	node.Children = make([]uint64, childCount)

	for i := uint8(0); i < childCount; i++ {
		child := uint64(0)
		if err := binary.Read(buf, binary.LittleEndian, &child); err != nil {
			return err
		}
		node.Children[i] = child
	}

	//self
	if err := binary.Read(buf, binary.LittleEndian, &node.Self); err != nil {
		return err
	}

	//next
	if err := binary.Read(buf, binary.LittleEndian, &node.Next); err != nil {
		return err
	}

	//prev
	if err := binary.Read(buf, binary.LittleEndian, &node.Prev); err != nil {
		return err
	}

	//parent
	if err := binary.Read(buf, binary.LittleEndian, &node.Parent); err != nil {
		return err
	}

	//keys
	keysCount := uint8(0)
	if err := binary.Read(buf, binary.LittleEndian, &keysCount); err != nil {
		return err
	}
	node.Keys = make([]string, keysCount)
	for i := uint8(0); i < keysCount; i++ {
		l := uint8(0)
		if err := binary.Read(buf, binary.LittleEndian, &l); err != nil {
			return err
		}
		v := make([]byte, l)
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return err
		}
		node.Keys[i] = string(v)
	}
	// Records
	recordCount := uint8(0)
	if err := binary.Read(buf, binary.LittleEndian, &recordCount); err != nil {
		return err
	}
	node.Records = make([]string, recordCount)
	for i := uint8(0); i < recordCount; i++ {
		l := uint8(0)
		if err := binary.Read(buf, binary.LittleEndian, &l); err != nil {
			return err
		}
		v := make([]byte, l)
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return err
		}
		node.Records[i] = string(v)
	}

	// IsLeaf
	if err := binary.Read(buf, binary.LittleEndian, &node.IsLeaf); err != nil {
		return err
	}

	return nil
}

func (t *Tree) writeNode(n *Node) error {
	if n == nil {
		return fmt.Errorf("flushNode == nil")
	}
	if t.File == nil {
		return fmt.Errorf("flush node into disk, but not open file")
	}

	var (
		length int
		err    error
	)

	bs := bytes.NewBuffer(make([]byte, 0))

	// IsActive
	if err = binary.Write(bs, binary.LittleEndian, n.IsDiskInTree); err != nil {
		return nil
	}

	// Children
	childCount := uint8(len(n.Children))
	if err = binary.Write(bs, binary.LittleEndian, childCount); err != nil {
		return err
	}
	for _, v := range n.Children {
		if err = binary.Write(bs, binary.LittleEndian, uint64(v)); err != nil {
			return err
		}
	}

	// Self
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Self)); err != nil {
		return err
	}

	// Next
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Next)); err != nil {
		return err
	}

	// Prev
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Prev)); err != nil {
		return err
	}

	// Parent
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Parent)); err != nil {
		return err
	}

	// Keys
	keysCount := uint8(len(n.Keys))
	if err = binary.Write(bs, binary.LittleEndian, keysCount); err != nil {
		return err
	}
	for _, v := range n.Keys {
		if err = binary.Write(bs, binary.LittleEndian, uint8(len([]byte(v)))); err != nil {
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	// Record
	recordCount := uint8(len(n.Records))
	if err = binary.Write(bs, binary.LittleEndian, recordCount); err != nil {
		return err
	}
	for _, v := range n.Records {
		if err = binary.Write(bs, binary.LittleEndian, uint8(len([]byte(v)))); err != nil {
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	// IsLeaf
	if err = binary.Write(bs, binary.LittleEndian, n.IsLeaf); err != nil {
		return err
	}

	dataLen := len(bs.Bytes())
	if uint64(dataLen)+8 > t.BlockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen)+4, t.BlockSize)
	}
	tmpbs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(tmpbs, binary.LittleEndian, uint64(dataLen)); err != nil {
		return err
	}

	data := append(tmpbs.Bytes(), bs.Bytes()...)
	if length, err = t.File.WriteAt(data, int64(n.Self)); err != nil {
		return err
	} else if len(data) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(n.Self), t.File.Name(), len(data), length)
	}
	return nil
}
