# LevelDB Benchmarks in Go

Benchmark LevelDB implementations in Go using same logic.

## Benchmarks

In labtop: MacBook Pro (Retina, 13-inch, Mid 2014), 3 GHz Intel Core i7, 16 GB 1600 MHz DDR3.

There are about about 10 GB free memory and no swap used in testing.

Options: No sync, No bloom filter, Snappy compression.

The result may not be accurate, and may contain errors. If in doubt, test it youself. Read the fucking
code if necessary.

There may be suttle differences in read/write options between implementations.
Currently, kezhuw/leveldb doest not write info log about LevelDB internal progress.

```shell
# go test -driver cpp -bench .
# https://github.com/google/leveldb  [cgo]
BenchmarkOpen-4                 10000000               234 ns/op
BenchmarkSeekRandom-4            1000000             47884 ns/op
BenchmarkReadHot-4                500000              4914 ns/op
BenchmarkReadRandom-4             500000              8081 ns/op
BenchmarkReadMissing-4            500000              7672 ns/op
BenchmarkReadReverse-4           1000000              1946 ns/op
BenchmarkReadSequential-4        1000000              1790 ns/op
BenchmarkWriteRandom-4            200000              6220 ns/op
BenchmarkWriteSequential-4        200000              5479 ns/op
BenchmarkDeleteRandom-4           300000              5689 ns/op
BenchmarkDeleteSequential-4       300000              4974 ns/op
ok      github.com/kezhuw/go-leveldb-benchmarks 158.519s
```

```shell
# go test -driver kezhuw -bench .
# https://github.com/kezhuw/leveldb
BenchmarkOpen-4                  1000000              2988 ns/op
BenchmarkSeekRandom-4            1000000             30365 ns/op
BenchmarkReadHot-4               1000000              7372 ns/op
BenchmarkReadRandom-4            1000000              8609 ns/op
BenchmarkReadMissing-4           1000000             15768 ns/op
BenchmarkReadReverse-4           1000000              1053 ns/op
BenchmarkReadSequential-4        2000000               947 ns/op
BenchmarkWriteRandom-4            200000              8073 ns/op
BenchmarkWriteSequential-4        200000              7148 ns/op
BenchmarkDeleteRandom-4           200000              9972 ns/op
BenchmarkDeleteSequential-4       200000              7756 ns/op
ok      github.com/kezhuw/go-leveldb-benchmarks 105.298s
```

```shell
# go test -driver syndtr -bench .
# https://github.com/syndtr/goleveldb
# Something wrong ?
BenchmarkOpen-4                  3000000               416 ns/op
BenchmarkSeekRandom-4             500000            117013 ns/op
BenchmarkReadHot-4                300000              6368 ns/op
BenchmarkReadRandom-4             300000             12658 ns/op
BenchmarkReadMissing-4            300000             12647 ns/op
BenchmarkReadReverse-4          SIGQUIT: quit
*** Test killed with quit: ran too long (10m0s).
FAIL    github.com/kezhuw/go-leveldb-benchmarks 600.154s
```

## License
The MIT License (MIT). See [LICENSE](LICENSE) for the full license text.
