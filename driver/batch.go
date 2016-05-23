package driver

type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Clear()
}
