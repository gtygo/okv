package bitcask

import "errors"

var (
	ErrNotFound=errors.New("Not Found ")

	ErrCRC32=errors.New("Checksum IEEE error ")

	ErrReadFailed=errors.New("Read BitCask Failed ")
)
