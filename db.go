package okv

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func getW(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}
func getR(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func set(k []byte, v []byte) error {
	f, _ := os.Create(string(k))
	_, err := f.Write((v))
	return err
}

func get(k []byte) ([]byte, error) {

	a, err := ioutil.ReadFile("./123")
	if err != nil {
		return nil, err
	}
	fmt.Println(a)

	return a, nil
}
