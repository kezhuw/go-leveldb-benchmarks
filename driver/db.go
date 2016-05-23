package driver

type DB interface {
	Get(key []byte, opts *ReadOptions) ([]byte, error)

	Put(key, value []byte, opts *WriteOptions) error
	Delete(key []byte, opts *WriteOptions) error
	Write(batch Batch, opts *WriteOptions) error

	All(opts *ReadOptions) Iterator

	Close() error

	IsCorrupt(err error) bool
	IsNotFound(err error) bool

	Batch() Batch
}
