package cgo

import (
	"errors"
	"runtime"
	"unsafe"

	"github.com/kezhuw/go-leveldb-benchmarks/driver"
)

// #cgo CFLAGS: -I/usr/include -I/usr/local/include
// #cgo LDFLAGS: -L/usr/lib -L/usr/local/lib -lleveldb
// #include <leveldb/c.h>
// #include <stdlib.h>
import "C"

var ErrNotFound = errors.New("leveldb: key not found")

type DB struct {
	db     *C.leveldb_t
	filter *C.leveldb_filterpolicy_t
	cache  *C.leveldb_cache_t
}

func bool2uchar(b bool) C.uchar {
	if b {
		return 1
	}
	return 0
}

func uchar2bool(u C.uchar) bool {
	if u == 0 {
		return false
	}
	return true
}

func str2error(str *C.char) error {
	defer C.free(unsafe.Pointer(str))
	return errors.New(C.GoString(str))
}

func bytes2chars(b []byte) (*C.char, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}

type iterator struct {
	it  *C.leveldb_iterator_t
	run bool
}

func (it *iterator) First() bool {
	it.run = true
	C.leveldb_iter_seek_to_first(it.it)
	return it.Valid()
}

func (it *iterator) Valid() bool {
	return uchar2bool(C.leveldb_iter_valid(it.it))
}

func (it *iterator) Last() bool {
	it.run = true
	C.leveldb_iter_seek_to_last(it.it)
	return it.Valid()
}

func (it *iterator) Next() bool {
	if !it.run {
		return it.First()
	}
	C.leveldb_iter_next(it.it)
	return it.Valid()
}

func (it *iterator) Prev() bool {
	if !it.run {
		return it.Last()
	}
	C.leveldb_iter_prev(it.it)
	return it.Valid()
}

func (it *iterator) Seek(key []byte) bool {
	it.run = true
	kp, kn := bytes2chars(key)
	C.leveldb_iter_seek(it.it, kp, kn)
	return it.Valid()
}

func (it *iterator) Key() []byte {
	var n C.size_t
	p := C.leveldb_iter_key(it.it, &n)
	if p == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(p), C.int(n))
}

func (it *iterator) Value() []byte {
	var n C.size_t
	p := C.leveldb_iter_value(it.it, &n)
	if p == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(p), C.int(n))
}

func (it *iterator) Close() error {
	err := it.Err()
	C.leveldb_iter_destroy(it.it)
	return err
}

func (it *iterator) Err() error {
	var errstr *C.char
	C.leveldb_iter_get_error(it.it, &errstr)
	if errstr == nil {
		return nil
	}
	return str2error(errstr)
}

type batch struct {
	batch *C.leveldb_writebatch_t
}

func (b *batch) Put(key, value []byte) {
	kp, kn := bytes2chars(key)
	vp, vn := bytes2chars(value)
	C.leveldb_writebatch_put(b.batch, kp, kn, vp, vn)
}

func (b *batch) Delete(key []byte) {
	kp, kn := bytes2chars(key)
	C.leveldb_writebatch_delete(b.batch, kp, kn)
}

func (b *batch) Clear() {
	C.leveldb_writebatch_clear(b.batch)
}

func (b *batch) finalize() {
	C.leveldb_writebatch_destroy(b.batch)
}

func (db *DB) Batch() driver.Batch {
	b := &batch{C.leveldb_writebatch_create()}
	runtime.SetFinalizer(b, (*batch).finalize)
	return b
}

func (db *DB) All(opts *driver.ReadOptions) driver.Iterator {
	copts := convertReadOptions(opts)
	defer C.leveldb_readoptions_destroy(copts)
	it := C.leveldb_create_iterator(db.db, copts)
	return &iterator{it: it}
}

func (db *DB) Get(key []byte, opts *driver.ReadOptions) ([]byte, error) {
	copts := convertReadOptions(opts)
	defer C.leveldb_readoptions_destroy(copts)
	var errstr *C.char
	var valueLen C.size_t
	kp, kn := bytes2chars(key)
	value := C.leveldb_get(db.db, copts, kp, kn, &valueLen, &errstr)
	if errstr != nil {
		return nil, str2error(errstr)
	}
	if value == nil {
		return nil, ErrNotFound
	}
	defer C.free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(valueLen)), nil
}

func (db *DB) Put(key, value []byte, opts *driver.WriteOptions) error {
	copts := convertWriteOptions(opts)
	defer C.leveldb_writeoptions_destroy(copts)
	var errstr *C.char
	kp, kn := bytes2chars(key)
	vp, vn := bytes2chars(value)
	C.leveldb_put(db.db, copts, kp, kn, vp, vn, &errstr)
	if errstr != nil {
		return str2error(errstr)
	}
	return nil
}

func (db *DB) Delete(key []byte, opts *driver.WriteOptions) error {
	copts := convertWriteOptions(opts)
	defer C.leveldb_writeoptions_destroy(copts)
	var errstr *C.char
	kp, kn := bytes2chars(key)
	C.leveldb_delete(db.db, copts, kp, kn, &errstr)
	if errstr != nil {
		return str2error(errstr)
	}
	return nil
}

func (db *DB) Write(writes driver.Batch, opts *driver.WriteOptions) error {
	copts := convertWriteOptions(opts)
	defer C.leveldb_writeoptions_destroy(copts)
	var errstr *C.char
	C.leveldb_write(db.db, copts, writes.(*batch).batch, &errstr)
	if errstr != nil {
		return str2error(errstr)
	}
	return nil
}

func (db *DB) Close() error {
	C.leveldb_close(db.db)
	if db.cache != nil {
		C.leveldb_cache_destroy(db.cache)
		db.cache = nil
	}
	if db.filter != nil {
		C.leveldb_filterpolicy_destroy(db.filter)
		db.filter = nil
	}
	return nil
}

func (db *DB) IsNotFound(err error) bool {
	return err == ErrNotFound
}

func (db *DB) IsCorrupt(err error) bool {
	return false
}

func convertOptions(dopts *driver.Options) (*C.leveldb_options_t, *C.leveldb_filterpolicy_t, *C.leveldb_cache_t) {
	copts := C.leveldb_options_create()
	if dopts.MaxOpenFiles > 0 {
		C.leveldb_options_set_max_open_files(copts, C.int(dopts.MaxOpenFiles))
	}
	var filter *C.leveldb_filterpolicy_t
	if dopts.BloomBitsPerKey > 0 {
		filter = C.leveldb_filterpolicy_create_bloom(C.int(dopts.BloomBitsPerKey))
		C.leveldb_options_set_filter_policy(copts, filter)
	}
	if dopts.WriteBufferSize > 0 {
		C.leveldb_options_set_write_buffer_size(copts, C.size_t(dopts.WriteBufferSize))
	}
	var cache *C.leveldb_cache_t
	if dopts.BlockCacheCapacity > 0 {
		cache = C.leveldb_cache_create_lru(C.size_t(dopts.BlockCacheCapacity))
		C.leveldb_options_set_cache(copts, cache)
	}
	C.leveldb_options_set_create_if_missing(copts, bool2uchar(dopts.CreateIfMissing))
	C.leveldb_options_set_error_if_exists(copts, bool2uchar(dopts.ErrorIfExists))
	switch dopts.Compression {
	case driver.NoCompression:
		C.leveldb_options_set_compression(copts, C.leveldb_no_compression)
	case driver.SnappyCompression:
		C.leveldb_options_set_compression(copts, C.leveldb_snappy_compression)
	}
	return copts, filter, cache
}

func convertReadOptions(dopts *driver.ReadOptions) *C.leveldb_readoptions_t {
	copts := C.leveldb_readoptions_create()
	if dopts != nil {
		if dopts.DontFillCache {
			C.leveldb_readoptions_set_fill_cache(copts, 0)
		}
		if dopts.VerifyChecksums {
			C.leveldb_readoptions_set_verify_checksums(copts, 1)
		}
	}
	return copts
}

func convertWriteOptions(dopts *driver.WriteOptions) *C.leveldb_writeoptions_t {
	copts := C.leveldb_writeoptions_create()
	if dopts != nil {
		C.leveldb_writeoptions_set_sync(copts, bool2uchar(dopts.Sync))
	}
	return copts
}

type driverType struct {
}

func (driverType) Open(dir string, dopts *driver.Options) (driver.DB, error) {
	copts, filter, cache := convertOptions(dopts)
	defer C.leveldb_options_destroy(copts)
	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))
	var errstr *C.char
	db := C.leveldb_open(copts, cdir, &errstr)
	if db == nil {
		C.leveldb_filterpolicy_destroy(filter)
		C.leveldb_cache_destroy(cache)
		return nil, str2error(errstr)
	}
	return &DB{db: db, filter: filter, cache: cache}, nil
}

func init() {
	driver.Register("cgo", driverType{})
}
