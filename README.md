# Index Benchmark

![Unit Tests](https://github.com/dbgroup-nagoya-u/index-benchmark/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
cd <path_to_your_workspace>
git clone --recursively git@github.com:dbgroup-nagoya-u/index-benchmark.git
```

### Build Options

- `INDEX_BENCH_BUILD_TESTS`: build unit tests if `on` (default: `off`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DINDEX_BENCH_BUILD_TESTS=on ..
make -j
ctest -C Release
```

## Usage

The following command displays available CLI options:

```bash
./index_bench --helpshort
```

For example, if you want to measure throughput with 8 threads, execute the following command:

```bash
./index_bench --throughput --num-thread 8
```

We prepare scripts in `bin` directory to measure performance with a variety of parameters. You can set parameters for benchmarking by `config/bench.env`.
