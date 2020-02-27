package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"time"
)

func checkWriteableFile(bc *BitCask) {
	if bc.activeFile.Offset  > bc.cfg.MaxFileSize && bc.activeFile.fileId != uint32(time.Now().Unix()) {
		//close data/hint fp
		bc.activeFile.hintFile.Close()
		bc.activeFile.file.Close()

		fileID ,writeFp:= setActiveFile(0, bc.cfg.FileDir)
		hintFp := setHintFile(fileID, bc.cfg.FileDir)
		bf := &File{
			file:writeFp,
			hintFile :hintFp,
			fileId :fileID,
			Offset:0,

		}
		bc.activeFile = bf
		// update pid
		writePID(bc.lockFile, fileID)
	}
}

func hasSuffix(src string,suffixs []string)bool{
	for i:=0;i<len(suffixs);i++{
		if b:=strings.HasSuffix(src,suffixs[i]);b{
			return true
		}
	}
	return false
}

func setHintFile(fileID uint32, dirName string) *os.File {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + ".hint"
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return fp
}

// lock a file by fp locker; the file must exits
func lockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_EXCL|os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func setActiveFile(fileID uint32, dirName string) (uint32,*os.File) {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + ".data"
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		fmt.Println("this will be panic ")
		panic(err)
	}
	fmt.Println("create file done. ",fp.Name())
	return fileID,fp
}

// get file last hint file info
func getLastHintFile(files []*os.File) (uint32, *os.File) {
	if files == nil {
		return uint32(0), nil
	}
	lastFp := files[0]

	fileName := lastFp.Name()
	s := strings.LastIndex(fileName, "/") + 1
	e := strings.LastIndex(fileName, ".hint")
	idx, _ := strconv.Atoi(fileName[s:e])
	lastID := idx
	for i := 0; i < len(files); i++ {
		idxFp := files[i]
		fileName = idxFp.Name()
		s = strings.LastIndex(fileName, "/") + 1
		e = strings.LastIndex(fileName, ".hint")
		idx, _ = strconv.Atoi(fileName[s:e])
		if lastID < idx {
			lastID = idx
			lastFp = idxFp
		}
	}
	return uint32(lastID), lastFp
}


func closeUnusedHintFile(files []*os.File, fileID uint32) {
	for _, fp := range files {
		if !strings.Contains(fp.Name(), strconv.Itoa(int(fileID))) {
			fp.Close()
		}
	}
}

func writePID(pidFp *os.File, fileID uint32) {
	pidFp.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\t"+strconv.Itoa(int(fileID))+".data"), 0)
}



func encodeHintFile(tStamp,kSize,vSize uint32 ,vOffset uint64,key []byte)[]byte{
	buf:=make([]byte,HintSizeWithoutK+len(key),HintSizeWithoutK+len(key))
	binary.LittleEndian.PutUint32(buf[0:4],tStamp)
	binary.LittleEndian.PutUint32(buf[4:8],kSize)
	binary.LittleEndian.PutUint32(buf[8:12],vSize)
	binary.LittleEndian.PutUint64(buf[12:HintSizeWithoutK],vOffset)
	copy(buf[HintSizeWithoutK:],key)
	return buf
}



func decodeHintFile(buf []byte)(uint32,uint32,uint32,uint64){
	return binary.LittleEndian.Uint32(buf[:4]),
	binary.LittleEndian.Uint32(buf[4:8]),
	binary.LittleEndian.Uint32(buf[8:12]),
	binary.LittleEndian.Uint64(buf[12:HintSizeWithoutK])
}

func encodeItem(tStamp, keySize, valueSize uint32, key, value []byte) []byte {
	bufSize := ItemSizeWithoutKV + keySize + valueSize
	buf := make([]byte, bufSize)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	copy(buf[ItemSizeWithoutKV:(ItemSizeWithoutKV+keySize)], key)
	copy(buf[(ItemSizeWithoutKV+keySize):(ItemSizeWithoutKV+keySize+valueSize)], value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], uint32(c32))
	return buf
}

func DecodeItem(buf []byte) ([]byte, error) {
	ksz := binary.LittleEndian.Uint32(buf[8:12])

	valuesz := binary.LittleEndian.Uint32(buf[12:ItemSizeWithoutKV])
	c32 := binary.LittleEndian.Uint32(buf[:4])
	value := make([]byte, valuesz)
	copy(value, buf[(ItemSizeWithoutKV+ksz):(ItemSizeWithoutKV+ksz+valuesz)])
	if crc32.ChecksumIEEE(buf[4:]) != c32 {
		return nil, ErrCRC32
	}
	return value, nil
}

func appendWriteFile(fp *os.File, buf []byte) (int, error) {
	stat, err := fp.Stat()
	if err != nil {
		return -1, err
	}

	return fp.WriteAt(buf, stat.Size())
}

// return a unique not exists file name by timeStamp
func uniqueFileName(root, suffix string) string {
	for {
		tStamp := strconv.Itoa(int(time.Now().Unix()))
		_, err := os.Stat(root + "/" + tStamp + "." + suffix)
		if err != nil && os.IsNotExist(err) {
			return tStamp + "." + suffix
		}
		time.Sleep(time.Second)
	}
}

