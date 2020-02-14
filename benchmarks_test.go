package leveldb_test

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/kezhuw/go-leveldb-benchmarks/cgo"
	"github.com/kezhuw/go-leveldb-benchmarks/driver"
	_ "github.com/kezhuw/go-leveldb-benchmarks/kezhuw"
	_ "github.com/kezhuw/go-leveldb-benchmarks/syndtr"
)

var driverName = flag.String("driver", "kezhuw", "Name of LevelDB implementation")

var openDBSize = flag.Int("open_db_size", 128*1024*1024, "Number of entries in db for opening")
var writeSync = flag.Bool("write_sync", false, "sync writing")
var cacheSize = flag.Int("cache_size", 0, "Capacity for block cache")
var valueSize = flag.Int("value_size", 100, "Size of each value")
var batchCount = flag.Int("batch_count", 1, "Batch count per write")

var writeBufferSize = flag.Int("write_buffer_size", 0, "Write buffer size")
var bloomBits = flag.Int("bloom_bits", 0, "Bits per key for bloom filter")
var openFiles = flag.Int("open_files", 0, "Max number of open files")

var compression = flag.String("compression", "default", "")
var compressionRatio = flag.Float64("compression_ratio", 0.5, "")

var maxConcurrency = flag.Int("max_concurrency", 2048, "Max concurrency in concurrent benchmark")

var openOptions driver.Options
var createOptions driver.Options

var readOptions driver.ReadOptions
var writeOptions driver.WriteOptions

func initOptions() {
	openOptions.MaxOpenFiles = *openFiles
	openOptions.BlockCacheCapacity = *cacheSize
	openOptions.WriteBufferSize = *writeBufferSize
	openOptions.BloomBitsPerKey = *bloomBits
	switch *compression {
	case "none":
		openOptions.Compression = driver.NoCompression
	case "snappy":
		openOptions.Compression = driver.SnappyCompression
	default:
		openOptions.Compression = driver.DefaultCompression
	}

	createOptions = openOptions
	createOptions.CreateIfMissing = true
	createOptions.ErrorIfExists = true

	writeOptions.Sync = *writeSync
}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = ' ' + byte(r.Intn('~'-' '+1))
	}
	return b
}

func compressibleBytes(r *rand.Rand, ratio float64, n int) []byte {
	m := maxInt(int(float64(n)*ratio), 1)
	p := randomBytes(r, m)
	b := make([]byte, 0, n+n%m)
	for len(b) < n {
		b = append(b, p...)
	}
	return b[:n]
}

type randomValueGenerator struct {
	b []byte
	k int
}

func (g *randomValueGenerator) Value(i int) []byte {
	i = (i * g.k) % len(g.b)
	return g.b[i : i+g.k]
}

func makeRandomValueGenerator(r *rand.Rand, ratio float64, valueSize int) randomValueGenerator {
	b := compressibleBytes(r, ratio, valueSize)
	max := maxInt(valueSize, 1024*1024)
	for len(b) < max {
		b = append(b, compressibleBytes(r, ratio, valueSize)...)
	}
	return randomValueGenerator{b: b, k: valueSize}
}

type entryGenerator interface {
	Key(i int) []byte
	Value(i int) []byte
}

type pairedEntryGenerator struct {
	keyGenerator
	randomValueGenerator
}

func newRandomEntryGenerator(n int) entryGenerator {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return &pairedEntryGenerator{
		keyGenerator:         newRandomKeyGenerator(n),
		randomValueGenerator: makeRandomValueGenerator(r, *compressionRatio, *valueSize),
	}
}

func newFullRandomEntryGenerator(start, n int) entryGenerator {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return &pairedEntryGenerator{
		keyGenerator:         newFullRandomKeyGenerator(start, n),
		randomValueGenerator: makeRandomValueGenerator(r, *compressionRatio, *valueSize),
	}
}

func newSequentialEntryGenerator(n int) entryGenerator {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return &pairedEntryGenerator{
		keyGenerator:         newSequentialKeyGenerator(n),
		randomValueGenerator: makeRandomValueGenerator(r, *compressionRatio, *valueSize),
	}
}

type keyGenerator interface {
	Key(i int) []byte
}

type randomKeyGenerator struct {
	n int
	b bytes.Buffer
	f string
	r *rand.Rand
}

func (g *randomKeyGenerator) Key(i int) []byte {
	i = g.r.Intn(g.n)
	g.b.Reset()
	fmt.Fprintf(&g.b, g.f, i)
	return g.b.Bytes()
}

func newRandomKeyGenerator(n int) keyGenerator {
	return &randomKeyGenerator{n: n, f: "%016d", r: rand.New(rand.NewSource(time.Now().Unix()))}
}

func newMissingKeyGenerator(n int) keyGenerator {
	return &randomKeyGenerator{n: n, f: "%016d.", r: rand.New(rand.NewSource(time.Now().Unix()))}
}

type fullRandomKeyGenerator struct {
	keys []int
	b    bytes.Buffer
}

func newFullRandomKeyGenerator(start, n int) keyGenerator {
	keys := make([]int, n)
	for i := 0; i < n; i++ {
		keys[i] = start + i
	}
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < n; i++ {
		j := r.Intn(n)
		keys[i], keys[j] = keys[j], keys[i]
	}
	return &fullRandomKeyGenerator{keys: keys}
}

func (g *fullRandomKeyGenerator) Key(i int) []byte {
	i = i % len(g.keys)
	i = g.keys[i]
	g.b.Reset()
	fmt.Fprintf(&g.b, "%016d", i)
	return g.b.Bytes()
}

type sequentialKeyGenerator struct {
	bytes.Buffer
}

func (g *sequentialKeyGenerator) Key(i int) []byte {
	g.Reset()
	fmt.Fprintf(g, "%016d", i)
	return g.Bytes()
}

func newSequentialKeyGenerator(n int) keyGenerator {
	return &sequentialKeyGenerator{}
}

func maxInt(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

func doRead(b *testing.B, db driver.DB, g keyGenerator, allowNotFound bool) {
	for i := 0; i < b.N; i++ {
		_, err := db.Get(g.Key(i), &readOptions)
		switch {
		case err == nil:
		case allowNotFound && db.IsNotFound(err):
		default:
			b.Fatalf("db get error: %s\n", err)
		}
	}
}

func doWrite(b *testing.B, db driver.DB, batchCount int, g entryGenerator) {
	batch := db.Batch()
	for i := 0; i < b.N; i += batchCount {
		for j := 0; j < batchCount; j++ {
			batch.Put(g.Key(i+j), g.Value(i+j))
		}
		err := db.Write(batch, &writeOptions)
		if err != nil {
			b.Fatalf("write db error: %s, type: %s\n", err, reflect.TypeOf(err))
		}
		batch.Clear()
	}
}

func doDelete(b *testing.B, db driver.DB, k int, g keyGenerator) {
	batch := db.Batch()
	for i := 0; i < b.N; i += k {
		for j := 0; j < k; j++ {
			batch.Delete(g.Key(i + j))
		}
		err := db.Write(batch, &writeOptions)
		if err != nil {
			b.Fatalf("db write error: %s\n", err)
		}
		batch.Clear()
	}
}

func createDB(b *testing.B) (driver.DB, string) {
	dir, err := ioutil.TempDir("", "leveldb-benchmark-")
	if err != nil {
		b.Fatalf("temp dir create error: %s", err)
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(dir)
		}
	}()
	db, err := driver.Open(*driverName, dir, &createOptions)
	if err != nil {
		b.Fatalf("create db %q error: %s\n", dir, err)
	}
	ok = true
	return db, dir
}

func newDB(b *testing.B) string {
	db, dir := createDB(b)
	defer runtime.GC()
	defer func() {
		if db != nil {
			db.Close()
			os.RemoveAll(dir)
		}
	}()
	doWrite(b, db, 1000, newFullRandomEntryGenerator(0, b.N))
	db.Close()
	db = nil
	return dir
}

func openDB(dir string, b *testing.B) driver.DB {
	db, err := driver.Open(*driverName, dir, &openOptions)
	if err != nil {
		b.Fatalf("open db %q error: %s\n", dir, err)
	}
	return db
}

func openFullDB(b *testing.B) (driver.DB, func()) {
	defer runtime.GC()
	defer b.ResetTimer()
	dir := newDB(b)
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(dir)
		}
	}()
	db := openDB(dir, b)
	ok = true
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func openEmptyDB(b *testing.B) (driver.DB, func()) {
	defer b.ResetTimer()
	db, dir := createDB(b)
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func BenchmarkOpen(b *testing.B) {
	defer func(N int) {
		b.N = N
	}(b.N)
	n := b.N
	b.N = *openDBSize / *valueSize
	dir := newDB(b)
	b.N = n
	defer os.RemoveAll(dir)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		openDB(dir, b).Close()
	}
}

func BenchmarkSeekRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	it := db.All(nil)
	defer it.Close()
	for i := 0; i < b.N; i++ {
		if !it.Seek(g.Key(i)) {
			b.Fatalf("db seek not found: %s\n", it.Err())
		}
	}
}

func BenchmarkReadHot(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	k := maxInt((b.N+99)/100, 1)
	g := newRandomKeyGenerator(k)
	doRead(b, db, g, false)
}

func BenchmarkReadRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	doRead(b, db, g, false)
}

func BenchmarkReadMissing(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newMissingKeyGenerator(b.N)
	doRead(b, db, g, true)
}

func BenchmarkReadReverse(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	it := db.All(nil)
	defer it.Close()
	for it.Prev() {
		it.Key()
		it.Value()
	}
	if err := it.Err(); err != nil {
		b.Fatalf("db iterator error: %s\n", err)
	}
}

func BenchmarkReadSequential(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	it := db.All(nil)
	defer it.Close()
	for it.Next() {
		it.Key()
		it.Value()
	}
	if err := it.Err(); err != nil {
		b.Fatalf("db iterator error: %s\n", err)
	}
}

func BenchmarkWriteRandom(b *testing.B) {
	db, cleanup := openEmptyDB(b)
	defer cleanup()
	g := newFullRandomEntryGenerator(0, b.N)
	b.ResetTimer()
	doWrite(b, db, maxInt(*batchCount, 1), g)
}

func BenchmarkWriteSequential(b *testing.B) {
	db, cleanup := openEmptyDB(b)
	defer cleanup()
	g := newSequentialEntryGenerator(b.N)
	b.ResetTimer()
	doWrite(b, db, maxInt(*batchCount, 1), g)
}

func buildConcurrentWrite(parallelism int) func(*testing.B) {
	return func(b *testing.B) {
		db, cleanup := openEmptyDB(b)
		defer cleanup()
		var gens []entryGenerator
		for i := 0; i < parallelism; i++ {
			gens = append(gens, newRandomEntryGenerator(b.N))
		}
		runtime.GC()
		b.ResetTimer()
		defer func(n int) {
			b.N = n
		}(b.N)
		b.N = (b.N + parallelism) / parallelism
		var wg sync.WaitGroup
		wg.Add(len(gens))
		for _, g := range gens {
			go func(g entryGenerator) {
				defer wg.Done()
				doWrite(b, db, maxInt(*batchCount, 1), g)
			}(g)
		}
		wg.Wait()
	}
}

func BenchmarkConcurrentWriteRandom(b *testing.B) {
	for i, n := 2, *maxConcurrency; i <= n; i *= 2 {
		name := fmt.Sprintf("parallelism-%d", i)
		runtime.GC()
		b.Run(name, buildConcurrentWrite(i))
	}
}

func BenchmarkDeleteRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	doDelete(b, db, maxInt(*batchCount, 1), newRandomKeyGenerator(b.N))
}

func BenchmarkDeleteSequential(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	doDelete(b, db, maxInt(*batchCount, 1), newSequentialKeyGenerator(b.N))
}

func TestMain(m *testing.M) {
	flag.Parse()
	initOptions()
	os.Exit(m.Run())
}
