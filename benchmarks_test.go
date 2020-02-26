package leveldb_test

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
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

const hitKeyFormat = "%016d+"
const missingKeyFormat = "%016d-"

var keyLen int

var templateDBDir string

func init() {
	var b bytes.Buffer
	keyLen, _ = fmt.Fprintf(&b, hitKeyFormat, math.MaxInt32)
	b.Reset()
	missingKeyLen, _ := fmt.Fprintf(&b, missingKeyFormat, math.MaxInt32)
	if keyLen != missingKeyLen {
		panic("len(key) != len(missingKey)")
	}
}

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
	keyGenerator
	Value(i int) []byte
}

type pairedEntryGenerator struct {
	keyGenerator
	randomValueGenerator
}

type startAtEntryGenerator struct {
	entryGenerator
	start int
}

var _ entryGenerator = (*startAtEntryGenerator)(nil)

func (g *startAtEntryGenerator) NKey() int {
	return g.entryGenerator.NKey() - g.start
}

func (g *startAtEntryGenerator) Key(i int) []byte {
	return g.entryGenerator.Key(g.start + i)
}

func newStartAtEntryGenerator(start int, g entryGenerator) entryGenerator {
	return &startAtEntryGenerator{start: start, entryGenerator: g}
}

func newSequentialKeys(n int, start int, keyFormat string) [][]byte {
	keys := make([][]byte, n)
	buffer := make([]byte, n*keyLen)
	for i := 0; i < n; i++ {
		begin, end := i*keyLen, (i+1)*keyLen
		key := buffer[begin:begin:end]
		n, _ := fmt.Fprintf(bytes.NewBuffer(key), keyFormat, start+i)
		if n != keyLen {
			panic("n != keyLen")
		}
		keys[i] = buffer[begin:end:end]
	}
	return keys
}

func newRandomKeys(n int, format string) [][]byte {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	keys := make([][]byte, n)
	buffer := make([]byte, n*keyLen)
	for i := 0; i < n; i++ {
		begin, end := i*keyLen, (i+1)*keyLen
		key := buffer[begin:begin:end]
		n, _ := fmt.Fprintf(bytes.NewBuffer(key), format, r.Intn(n))
		if n != keyLen {
			panic("n != keyLen")
		}
		keys[i] = buffer[begin:end:end]
	}
	return keys
}

func newFullRandomKeys(n int, start int, format string) [][]byte {
	keys := newSequentialKeys(n, start, format)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < n; i++ {
		j := r.Intn(n)
		keys[i], keys[j] = keys[j], keys[i]
	}
	return keys
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
	NKey() int
	Key(i int) []byte
}

type reversedKeyGenerator struct {
	keyGenerator
}

var _ keyGenerator = (*reversedKeyGenerator)(nil)

func (g *reversedKeyGenerator) Key(i int) []byte {
	return g.keyGenerator.Key(g.NKey() - i - 1)
}

func newReversedKeyGenerator(g keyGenerator) keyGenerator {
	return &reversedKeyGenerator{keyGenerator: g}
}

type roundKeyGenerator struct {
	keyGenerator
}

var _ keyGenerator = (*roundKeyGenerator)(nil)

func (g *roundKeyGenerator) Key(i int) []byte {
	return g.keyGenerator.Key(i % g.NKey())
}

func newRoundKeyGenerator(g keyGenerator) keyGenerator {
	return &roundKeyGenerator{keyGenerator: g}
}

type predefinedKeyGenerator struct {
	keys [][]byte
}

func (g *predefinedKeyGenerator) NKey() int {
	return len(g.keys)
}

func (g *predefinedKeyGenerator) Key(i int) []byte {
	return g.keys[i]
}

func newRandomKeyGenerator(n int) keyGenerator {
	return &predefinedKeyGenerator{keys: newRandomKeys(n, hitKeyFormat)}
}

func newRandomMissingKeyGenerator(n int) keyGenerator {
	return &predefinedKeyGenerator{keys: newRandomKeys(n, missingKeyFormat)}
}

func newFullRandomKeyGenerator(start, n int) keyGenerator {
	return &predefinedKeyGenerator{keys: newFullRandomKeys(n, start, hitKeyFormat)}
}

func newSortedRandomKeyGenerator(n int) keyGenerator {
	keys := newRandomKeys(n, hitKeyFormat)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	return &predefinedKeyGenerator{keys: keys}
}

func newSequentialKeyGenerator(n int) keyGenerator {
	return &predefinedKeyGenerator{keys: newSequentialKeys(n, 0, hitKeyFormat)}
}

func maxInt(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

func doRead(b *testing.B, db driver.DB, g keyGenerator, allowNotFound bool) {
	for i := 0; i < b.N; i++ {
		key := g.Key(i)
		_, err := db.Get(key, &readOptions)
		switch {
		case err == nil:
		case allowNotFound && db.IsNotFound(err):
		default:
			b.Fatalf("db get key[%s] error: %s\n", key, err)
		}
	}
}

type DBWriter interface {
	Put(key, value []byte)

	Delete(key []byte)

	Done()
}

type singularDBWriter struct {
	db driver.DB
}

var _ DBWriter = (*singularDBWriter)(nil)

func (w *singularDBWriter) Put(key, value []byte) {
	err := w.db.Put(key, value, &writeOptions)
	if err != nil {
		panic(err)
	}
}

func (w *singularDBWriter) Delete(key []byte) {
	err := w.db.Delete(key, &writeOptions)
	if err != nil {
		panic(err)
	}
}

func (w *singularDBWriter) Done() {
}

type batchDBWriter struct {
	db    driver.DB
	batch driver.Batch
	max   int
	count int
}

var _ DBWriter = (*batchDBWriter)(nil)

func (w *batchDBWriter) writeBatch() error {
	err := w.db.Write(w.batch, &writeOptions)
	w.count = 0
	w.batch.Clear()
	return err
}

func (w *batchDBWriter) checkBatch(max int) {
	if w.count >= max {
		err := w.db.Write(w.batch, &writeOptions)
		if err != nil {
			panic(err)
		}
		w.count = 0
		w.batch.Clear()
	}
}

func (w *batchDBWriter) Put(key, value []byte) {
	w.batch.Put(key, value)
	w.count++
	w.checkBatch(w.max)
}

func (w *batchDBWriter) Delete(key []byte) {
	w.batch.Delete(key)
	w.count++
	w.checkBatch(w.max)
}

func (w *batchDBWriter) Done() {
	w.checkBatch(1)
}

func newDBWriter(db driver.DB, batchCount int) DBWriter {
	if batchCount <= 1 {
		return &singularDBWriter{db: db}
	}
	return &batchDBWriter{
		db:    db,
		batch: db.Batch(),
		max:   batchCount,
		count: 0,
	}
}

func doWrite(db driver.DB, n int, batchCount int, g entryGenerator) {
	w := newDBWriter(db, batchCount)
	for i := 0; i < n; i++ {
		w.Put(g.Key(i), g.Value(i))
	}
	w.Done()
}

func doDelete(b *testing.B, db driver.DB, k int, g keyGenerator) {
	w := newDBWriter(db, k)
	for i := 0; i < b.N; i++ {
		w.Delete(g.Key(i))
	}
	w.Done()
}

func createDB(n int) (driver.DB, string) {
	dir, err := ioutil.TempDir("", "leveldb-benchmark-")
	if err != nil {
		panic(fmt.Errorf("temp dir create error: %s", err))
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(dir)
		}
	}()
	db, err := driver.Open(*driverName, dir, &createOptions)
	if err != nil {
		panic(fmt.Errorf("create db %q error: %s\n", dir, err))
	}
	ok = true
	return db, dir
}

func newDB(n int) string {
	db, dir := createDB(n)
	defer runtime.GC()
	defer func() {
		if db != nil {
			db.Close()
			os.RemoveAll(dir)
		}
	}()
	doWrite(db, n, 1000, newFullRandomEntryGenerator(0, n))
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
	dir := newDB(b.N)
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
	db, dir := createDB(b.N)
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func resetBenchmark(b *testing.B) {
	runtime.GC()
	b.ResetTimer()
}

func openTemplateDB() driver.DB {
	db, err := driver.Open(*driverName, templateDBDir, &openOptions)
	if err != nil {
		panic(err)
	}
	return db
}

func BenchmarkOpen(b *testing.B) {
	resetBenchmark(b)
	for i := 0; i < b.N; i++ {
		openTemplateDB().Close()
	}
}

func BenchmarkSeekRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	it := db.All(nil)
	defer it.Close()
	resetBenchmark(b)
	for i := 0; i < b.N; i++ {
		key := g.Key(i)
		if !it.Seek(key) {
			b.Fatalf("db seek key [%s] not found, error %s\n", key, it.Err())
		}
		if !bytes.Equal(key, it.Key()) {
			b.Fatalf("db seek key [%s] not found, got %s\n", key, it.Key())
		}
		it.Value()
	}
}

func BenchmarkReadHot(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	k := maxInt((b.N+99)/100, 1)
	g := newRoundKeyGenerator(newRandomKeyGenerator(k))
	resetBenchmark(b)
	doRead(b, db, g, false)
}

func BenchmarkReadRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	resetBenchmark(b)
	doRead(b, db, g, false)
}

func BenchmarkReadRandomMissing(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomMissingKeyGenerator(b.N)
	resetBenchmark(b)
	doRead(b, db, g, true)
}

func BenchmarkReadSequential(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newSequentialKeyGenerator(b.N)
	resetBenchmark(b)
	doRead(b, db, g, false)
}

func BenchmarkReadReverse(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newReversedKeyGenerator(newSequentialKeyGenerator(b.N))
	resetBenchmark(b)
	doRead(b, db, g, false)
}

func BenchmarkIterateSequential(b *testing.B) {
	db := openTemplateDB()
	defer db.Close()
	it := db.All(nil)
	defer it.Close()
	resetBenchmark(b)
	it.First()
	for i := 0; i < b.N; i++ {
		switch it.Valid() {
		case false:
			it.First()
		default:
			it.Key()
			it.Value()
			it.Next()
		}
	}
}

func BenchmarkIterateReverse(b *testing.B) {
	db := openTemplateDB()
	defer db.Close()
	it := db.All(nil)
	defer it.Close()
	resetBenchmark(b)
	it.Last()
	for i := 0; i < b.N; i++ {
		switch it.Valid() {
		case false:
			it.Last()
		default:
			it.Key()
			it.Value()
			it.Prev()
		}
	}
}

func BenchmarkWriteSequential(b *testing.B) {
	db, cleanup := openEmptyDB(b)
	defer cleanup()
	g := newSequentialEntryGenerator(b.N)
	resetBenchmark(b)
	doWrite(db, b.N, *batchCount, g)
}

func buildConcurrentWrite(parallelism int) func(*testing.B) {
	return func(b *testing.B) {
		db, cleanup := openEmptyDB(b)
		defer cleanup()
		var gens []entryGenerator
		start, step := 0, (b.N+parallelism)/parallelism
		n := step * parallelism
		g := newFullRandomEntryGenerator(0, n)
		for i := 0; i < parallelism; i++ {
			gens = append(gens, newStartAtEntryGenerator(start, g))
			start += step
		}
		defer func(n int) {
			b.N = n
		}(b.N)
		b.N = step
		resetBenchmark(b)
		var wg sync.WaitGroup
		wg.Add(len(gens))
		for _, g := range gens {
			go func(g entryGenerator) {
				defer wg.Done()
				doWrite(db, b.N, *batchCount, g)
			}(g)
		}
		wg.Wait()
	}
}

func BenchmarkWriteRandom(b *testing.B) {
	for i, n := 1, *maxConcurrency; i <= n; i *= 2 {
		name := fmt.Sprintf("parallelism-%d", i)
		runtime.GC()
		b.Run(name, buildConcurrentWrite(i))
	}
}

func BenchmarkDeleteRandom(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newRandomKeyGenerator(b.N)
	resetBenchmark(b)
	doDelete(b, db, maxInt(*batchCount, 1), g)
}

func BenchmarkDeleteSequential(b *testing.B) {
	db, cleanup := openFullDB(b)
	defer cleanup()
	g := newSortedRandomKeyGenerator(b.N)
	resetBenchmark(b)
	doDelete(b, db, maxInt(*batchCount, 1), g)
}

func TestMain(m *testing.M) {
	flag.Parse()
	initOptions()
	templateDBDir = newDB(*openDBSize / *valueSize)
	defer os.RemoveAll(templateDBDir)
	os.Exit(m.Run())
}
