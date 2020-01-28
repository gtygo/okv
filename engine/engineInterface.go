package engine

type engine interface {
	Insert(key string, val string) error
	Find(key string) (string, error)
	Update(key string, value string) error
	Delete(key string) error
}
