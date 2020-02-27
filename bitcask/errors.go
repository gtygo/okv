package bitcask

import "errors"

var (
	ErrNotFound=errors.New("Not Found ")

	ErrCRC32=errors.New("checksum IEEE error")

)
