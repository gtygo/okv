package bitcask

const (
	//default file size 1G
	DefaultMaxFileSize=(1<<32)-1
	DefaultMaxValueSize=(1<<8)-1
	DefaultFileDir="data"
)

type Config struct{
	MaxFileSize uint64
	MaxValueSize uint64
	FileDir string
}

func initConfig(maxFileSize uint64,fileDir string)*Config{
	if maxFileSize<0{
		maxFileSize=DefaultMaxFileSize
	}
	return &Config{
		MaxFileSize:  maxFileSize,
		MaxValueSize: DefaultMaxValueSize,
		FileDir:fileDir,
	}
}