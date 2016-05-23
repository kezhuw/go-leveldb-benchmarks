package driver

const (
	DefaultCompression = iota
	NoCompression
	SnappyCompression
)

type Options struct {
	Compression        int
	MaxOpenFiles       int
	BloomBitsPerKey    int
	WriteBufferSize    int
	BlockCacheCapacity int
	CreateIfMissing    bool
	ErrorIfExists      bool
}

type ReadOptions struct {
	DontFillCache   bool
	VerifyChecksums bool
}

type WriteOptions struct {
	Sync bool
}
