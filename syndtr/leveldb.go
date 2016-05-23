package syndtr

import (
	"github.com/kezhuw/go-leveldb-benchmarks/driver"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type DB struct {
	db *leveldb.DB
}

func convertOptions(dopts *driver.Options) *opt.Options {
	if dopts == nil {
		return nil
	}
	opts := &opt.Options{
		ErrorIfMissing:         !dopts.CreateIfMissing,
		ErrorIfExist:           dopts.ErrorIfExists,
		BlockCacheCapacity:     dopts.BlockCacheCapacity,
		OpenFilesCacheCapacity: dopts.MaxOpenFiles,
		WriteBuffer:            dopts.WriteBufferSize,
	}
	if dopts.BloomBitsPerKey > 0 {
		opts.Filter = filter.NewBloomFilter(dopts.BloomBitsPerKey)
	}
	switch dopts.Compression {
	case driver.NoCompression:
		opts.Compression = opt.NoCompression
	case driver.SnappyCompression:
		opts.Compression = opt.SnappyCompression
	}
	return opts
}

func convertReadOptions(dopts *driver.ReadOptions) *opt.ReadOptions {
	if dopts == nil || (!dopts.DontFillCache && !dopts.VerifyChecksums) {
		return nil
	}
	var opts opt.ReadOptions
	opts.DontFillCache = dopts.DontFillCache
	if dopts.VerifyChecksums {
		opts.Strict = opt.StrictBlockChecksum
	}
	return &opts
}

func convertWriteOptions(dopts *driver.WriteOptions) *opt.WriteOptions {
	return (*opt.WriteOptions)(dopts)
}

type batch struct {
	*leveldb.Batch
}

func (b batch) Clear() {
	b.Batch.Reset()
}

type wrappedIterator struct {
	iterator.Iterator
}

func (it *wrappedIterator) Err() error {
	return it.Error()
}

func (it *wrappedIterator) Release() error {
	it.Iterator.Release()
	return nil
}

func (db *DB) All(opts *driver.ReadOptions) driver.Iterator {
	it := db.db.NewIterator(nil, convertReadOptions(opts))
	return &wrappedIterator{it}
}

func (db *DB) Get(key []byte, opts *driver.ReadOptions) ([]byte, error) {
	return db.db.Get(key, convertReadOptions(opts))
}

func (db *DB) Put(key, value []byte, opts *driver.WriteOptions) error {
	return db.db.Put(key, value, convertWriteOptions(opts))
}

func (db *DB) Write(writes driver.Batch, opts *driver.WriteOptions) error {
	return db.db.Write(writes.(batch).Batch, convertWriteOptions(opts))
}

func (db *DB) Delete(key []byte, opts *driver.WriteOptions) error {
	return db.db.Delete(key, convertWriteOptions(opts))
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Batch() driver.Batch {
	return batch{new(leveldb.Batch)}
}

func (db *DB) IsNotFound(err error) bool {
	return err == leveldb.ErrNotFound
}

func (db *DB) IsCorrupt(err error) bool {
	return errors.IsCorrupted(err)
}

type driverType struct {
}

func (driverType) Open(dir string, opts *driver.Options) (driver.DB, error) {
	db, err := leveldb.OpenFile(dir, convertOptions(opts))
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func init() {
	driver.Register("syndtr", driverType{})
}
