package kezhuw

import (
	"github.com/kezhuw/go-leveldb-benchmarks/driver"
	"github.com/kezhuw/leveldb"
)

type DB struct {
	db *leveldb.DB
}

func (db *DB) Get(key []byte, opts *driver.ReadOptions) ([]byte, error) {
	return db.db.Get(key, convertReadOptions(opts))
}

func (db *DB) Put(key, value []byte, opts *driver.WriteOptions) error {
	return db.db.Put(key, value, convertWriteOptions(opts))
}

func (db *DB) Delete(key []byte, opts *driver.WriteOptions) error {
	return db.db.Delete(key, convertWriteOptions(opts))
}

func (db *DB) Write(batch driver.Batch, opts *driver.WriteOptions) error {
	return db.db.Write(*(batch.(*leveldb.Batch)), convertWriteOptions(opts))
}

func (db *DB) Batch() driver.Batch {
	return new(leveldb.Batch)
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) IsNotFound(err error) bool {
	return err == leveldb.ErrNotFound
}

func (db *DB) IsCorrupt(err error) bool {
	return leveldb.IsCorrupt(err)
}

func (db *DB) All(opts *driver.ReadOptions) driver.Iterator {
	return db.db.All(convertReadOptions(opts))
}

func convertOptions(dopts *driver.Options) *leveldb.Options {
	if dopts == nil {
		return nil
	}
	opts := &leveldb.Options{
		CreateIfMissing:    dopts.CreateIfMissing,
		ErrorIfExists:      dopts.ErrorIfExists,
		BlockCacheCapacity: dopts.BlockCacheCapacity,
		MaxOpenFiles:       dopts.MaxOpenFiles,
		WriteBufferSize:    dopts.WriteBufferSize,
	}
	if dopts.BloomBitsPerKey > 0 {
		opts.Filter = leveldb.NewBloomFilter(dopts.BloomBitsPerKey)
	}
	switch dopts.Compression {
	case driver.NoCompression:
		opts.Compression = leveldb.NoCompression
	case driver.SnappyCompression:
		opts.Compression = leveldb.SnappyCompression
	}
	return opts
}

func convertReadOptions(opts *driver.ReadOptions) *leveldb.ReadOptions {
	return (*leveldb.ReadOptions)(opts)
}

func convertWriteOptions(opts *driver.WriteOptions) *leveldb.WriteOptions {
	return (*leveldb.WriteOptions)(opts)
}

type driverType struct {
}

func (driverType) Open(dir string, opts *driver.Options) (driver.DB, error) {
	db, err := leveldb.Open(dir, convertOptions(opts))
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func init() {
	driver.Register("kezhuw", driverType{})
}
