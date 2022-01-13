# Index Benchmark

![Unit Tests](https://github.com/dbgroup-nagoya-u/index-benchmark/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
cd <path_to_your_workspace>
git clone --recursive git@github.com:dbgroup-nagoya-u/index-benchmark.git
```

### Build Options

#### Memory Allocation

- `INDEX_BENCH_OVERRIDE_JEMALLOC`: override entire memory allocation with jemalloc if `ON` (default: `OFF`).
    - We assume that jemalloc is configured with the following command.

    ```bash
    ./configure --prefix=/usr/local --with-version=VERSION
    ```

- `INDEX_BENCH_OVERRIDE_MIMALLOC`: override entire memory allocation with mimalloc if `ON` (default: `OFF`).

#### Optional Benchmarking Targets

- `INDEX_BENCH_BUILD_OPEN_BWTREE`: build a benchmarker with OpenBw-Tree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_MASSTREE`: build a benchmarker with Masstree if `ON` (default: `OFF`).
    - Note: when you turn on both `INDEX_BENCH_BUILD_MASSTREE` and `INDEX_BENCH_BUILD_OPEN_BWTREE`, the performance of Masstree decreases (we do not know what caused it).
- `INDEX_BENCH_BUILD_PTREE`: build a benchmarker with PTree if `ON` (default: `OFF`).
    - Note: when you measure the performance of indexes other than PTree, turn off this option because PTree reserves threads for parallel writing.

### Build Options for Unit Testing

- `INDEX_BENCH_BUILD_TESTS`: build unit tests if `ON` (default: `OFF`).
- `INDEX_BENCH_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `8`).

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
./index_bench --throughput=t --num-thread 8
```

If you want to measure latency with skewed keys (keys are generated according to Zipf's law), execute the following command:

```bash
./index_bench --throughput=f --skew-parameter 1.0
```

We prepare scripts in `bin` directory to measure performance with a variety of parameters. You can set parameters for benchmarking by `config/bench.env`.
