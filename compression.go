package okv

import "io"

type Compression interface {
	Writer(d io.Writer) (io.WriteCloser, error)
	Reader(s io.Reader) (io.ReadCloser, error)
}
