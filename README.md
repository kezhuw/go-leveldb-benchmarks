# LevelDB Benchmarks in Go

Benchmark LevelDB implementations in Go using same logic.

## Benchmarks

In labtop: MacBook Pro (Retina, 13-inch, Mid 2014), 3 GHz Intel Core i7, 16 GB 1600 MHz DDR3.

There are about 10 GB free memory and no swap used in testing.

Default options: No sync, No bloom filter, Snappy compression. The
result may not be accurate, and may contain errors. If in doubt,
test it youself.

```shell
# go test -driver cgo -bench .
# https://github.com/google/leveldb
BenchmarkOpen-4                     1000           1185245 ns/op
BenchmarkSeekRandom-4            1000000             42137 ns/op
BenchmarkReadHot-4                500000              5093 ns/op
BenchmarkReadRandom-4             300000              7475 ns/op
BenchmarkReadMissing-4            500000              7429 ns/op
BenchmarkReadReverse-4           1000000              2127 ns/op
BenchmarkReadSequential-4        1000000              1742 ns/op
BenchmarkWriteRandom-4            200000              7657 ns/op
BenchmarkWriteSequential-4        200000              6072 ns/op
BenchmarkDeleteRandom-4           300000              5910 ns/op
BenchmarkDeleteSequential-4       300000              5214 ns/op
ok      github.com/kezhuw/go-leveldb-benchmarks 85.146s
```

```shell
# go test -driver kezhuw -bench .
# https://github.com/kezhuw/leveldb
BenchmarkOpen-4                       10         237217132 ns/op
BenchmarkSeekRandom-4            1000000             31836 ns/op
BenchmarkReadHot-4               1000000              8012 ns/op
BenchmarkReadRandom-4             500000              7945 ns/op
BenchmarkReadMissing-4           1000000             10079 ns/op
BenchmarkReadReverse-4           1000000              1360 ns/op
BenchmarkReadSequential-4        1000000              1399 ns/op
BenchmarkWriteRandom-4            200000              8077 ns/op
BenchmarkWriteSequential-4        200000              6820 ns/op
BenchmarkDeleteRandom-4           200000             10113 ns/op
BenchmarkDeleteSequential-4       200000              8539 ns/op
ok      github.com/kezhuw/go-leveldb-benchmarks 96.735s
```

```shell
# go test -driver syndtr -bench .
# https://github.com/syndtr/goleveldb
BenchmarkOpen-4                      300           3576028 ns/op
BenchmarkSeekRandom-4             500000            117406 ns/op
BenchmarkReadHot-4                300000              7360 ns/op
BenchmarkReadRandom-4             300000             12988 ns/op
BenchmarkReadMissing-4            300000             13317 ns/op
BenchmarkReadReverse-4           2000000              1126 ns/op
BenchmarkReadSequential-4        2000000               955 ns/op
BenchmarkWriteRandom-4            200000              6103 ns/op
BenchmarkWriteSequential-4        300000              4611 ns/op
BenchmarkDeleteRandom-4           300000              5822 ns/op
BenchmarkDeleteSequential-4       300000              4864 ns/op
ok      github.com/kezhuw/go-leveldb-benchmarks 164.230s
```

## License
The MIT License (MIT). See [LICENSE](LICENSE) for the full license text.
