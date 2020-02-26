# LevelDB Benchmarks in Go

Benchmark LevelDB implementations in Go using same benchmarking code.

## Implementations
Currently, we benchmark three implementations:

* [google/leveldb](https://github.com/google/leveldb) Official C++ implementation.
* [syndtr/goleveldb](https://github.com/syndtr/goleveldb) Go implementation.
* [kezhuw/leveldb](https://github.com/kezhuw/leveldb) Go implementation.

## Benchmark Results
In labtop: MacBook Pro (Retina, 15-inch, Mid 2019), 2.3 GHz 8-Core Intel Core i9, 32 GB 2400 MHz DDR4, 1T SSD. There are about 20 GB free memory, 360 GB free SSD, and no swap used in testing.

Default options: No sync, No bloom filter, Snappy compression. The
results may not be accurate, and may contain errors. If in doubt,
test it youself.

```shell
# go test -driver cgo -bench .
# https://github.com/google/leveldb
BenchmarkOpen-16                              37          31493764 ns/op
BenchmarkSeekRandom-16                    670570             23396 ns/op
BenchmarkReadHot-16                       540703              2676 ns/op
BenchmarkReadRandom-16                    537300              3877 ns/op
BenchmarkReadRandomMissing-16             606057              4045 ns/op
BenchmarkReadSequential-16                544921              2481 ns/op
BenchmarkReadReverse-16                   541011              2483 ns/op
BenchmarkIterateSequential-16            1479330               792 ns/op
BenchmarkIterateReverse-16               1588371               709 ns/op
BenchmarkWriteSequential-16               246870              4381 ns/op
BenchmarkWriteRandom/parallelism-1-16             232010              5100 ns/op
BenchmarkWriteRandom/parallelism-2-16             205617              6036 ns/op
BenchmarkWriteRandom/parallelism-4-16             220036              5321 ns/op
BenchmarkWriteRandom/parallelism-8-16             240717              4984 ns/op
BenchmarkWriteRandom/parallelism-16-16            244486              4586 ns/op
BenchmarkWriteRandom/parallelism-32-16            249603              5343 ns/op
BenchmarkWriteRandom/parallelism-64-16            250314              5933 ns/op
BenchmarkWriteRandom/parallelism-128-16           243891              6141 ns/op
BenchmarkWriteRandom/parallelism-256-16           241074              7569 ns/op
BenchmarkWriteRandom/parallelism-512-16           243393              8754 ns/op
BenchmarkWriteRandom/parallelism-1024-16          206619             12532 ns/op
BenchmarkWriteRandom/parallelism-2048-16          188739             14001 ns/op
BenchmarkDeleteRandom-16                          249382              4478 ns/op
BenchmarkDeleteSequential-16                      260295              3985 ns/op
PASS
ok      github.com/kezhuw/go-leveldb-benchmarks 85.607s
```

```shell
# go test -driver kezhuw -bench .
# https://github.com/kezhuw/leveldb
BenchmarkOpen-16                               8         128231267 ns/op
BenchmarkSeekRandom-16                   1000000             11151 ns/op
BenchmarkReadHot-16                      1000000              3011 ns/op
BenchmarkReadRandom-16                   1000000              4549 ns/op
BenchmarkReadRandomMissing-16            1000000              4830 ns/op
BenchmarkReadSequential-16               1000000              3239 ns/op
BenchmarkReadReverse-16                  1000000              2893 ns/op
BenchmarkIterateSequential-16            2838139               354 ns/op
BenchmarkIterateReverse-16               2341484               468 ns/op
BenchmarkWriteSequential-16               158544              7345 ns/op
BenchmarkWriteRandom/parallelism-1-16             153738              8073 ns/op
BenchmarkWriteRandom/parallelism-2-16             163914              7732 ns/op
BenchmarkWriteRandom/parallelism-4-16             241588              7745 ns/op
BenchmarkWriteRandom/parallelism-8-16             292773              5369 ns/op
BenchmarkWriteRandom/parallelism-16-16            386944              3734 ns/op
BenchmarkWriteRandom/parallelism-32-16            512961              2688 ns/op
BenchmarkWriteRandom/parallelism-64-16            622414              2376 ns/op
BenchmarkWriteRandom/parallelism-128-16           726721              2030 ns/op
BenchmarkWriteRandom/parallelism-256-16           737936              2090 ns/op
BenchmarkWriteRandom/parallelism-512-16           789744              1929 ns/op
BenchmarkWriteRandom/parallelism-1024-16          813272              2001 ns/op
BenchmarkWriteRandom/parallelism-2048-16          753675              2578 ns/op
BenchmarkDeleteRandom-16                          164695              7878 ns/op
BenchmarkDeleteSequential-16                      184504              6833 ns/op
PASS
ok      github.com/kezhuw/go-leveldb-benchmarks 85.923s
```

```shell
# go test -driver syndtr -bench .
# https://github.com/syndtr/goleveldb
BenchmarkOpen-16                              38          31544791 ns/op
BenchmarkSeekRandom-16                    619424             40477 ns/op
BenchmarkReadHot-16                       442094              3441 ns/op
BenchmarkReadRandom-16                    410964              6165 ns/op
BenchmarkReadRandomMissing-16             411609              6291 ns/op
BenchmarkReadSequential-16                441164              3254 ns/op
BenchmarkReadReverse-16                   430430              3259 ns/op
BenchmarkIterateSequential-16            2236461               460 ns/op
BenchmarkIterateReverse-16               2163050               518 ns/op
BenchmarkWriteSequential-16               227752              4572 ns/op
BenchmarkWriteRandom/parallelism-1-16             225344              5160 ns/op
BenchmarkWriteRandom/parallelism-2-16             215760              5742 ns/op
BenchmarkWriteRandom/parallelism-4-16             268276              4616 ns/op
BenchmarkWriteRandom/parallelism-8-16             292522              3959 ns/op
BenchmarkWriteRandom/parallelism-16-16            366172              3385 ns/op
BenchmarkWriteRandom/parallelism-32-16            372770              3350 ns/op
BenchmarkWriteRandom/parallelism-64-16            350263              3629 ns/op
BenchmarkWriteRandom/parallelism-128-16           290047              4598 ns/op
BenchmarkWriteRandom/parallelism-256-16           249349              5071 ns/op
BenchmarkWriteRandom/parallelism-512-16           233002              5083 ns/op
BenchmarkWriteRandom/parallelism-1024-16          235010              5491 ns/op
BenchmarkWriteRandom/parallelism-2048-16          189348              5854 ns/op
BenchmarkDeleteRandom-16                          238869              5981 ns/op
BenchmarkDeleteSequential-16                      257317              5015 ns/op
PASS
ok      github.com/kezhuw/go-leveldb-benchmarks 141.233s
```

## License
The MIT License (MIT). See [LICENSE](LICENSE) for the full license text.
