# Index Benchmark

[![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/index-benchmark/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/index-benchmark/actions/workflows/unit_tests.yaml)

## Build

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake libgflags-dev libtbb-dev
cd <path_to_your_workspace>
git clone --recursive git@github.com:dbgroup-nagoya-u/index-benchmark.git
```

### Build Options

#### Utility Options

- `INDEX_BENCH_BUILD_LONG_KEYS`: build keys with sizes of 16/32/64/128 bytes if `ON` (default: `OFF`).
- `INDEX_BENCH_PAGE_SIZE`: set the default page size for B+trees in bytes if needed.
    - Our B+trees use 1,024 bytes as default.
- `INDEX_BENCH_MAX_DELTA_RECORD_NUM`: set the maximum number of delta records if needed.
    - NOTE: our Bw-tree and BzTree use different defaults, so please refer to each repository.

#### Memory Allocation

- `INDEX_BENCH_OVERRIDE_MIMALLOC`: override entire memory allocation with mimalloc if `ON` (default: `OFF`).

#### Optional Benchmarking Targets

- `INDEX_BENCH_BUILD_BTREE_OLC`: build a benchmarker with OLC based B+tree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_OPEN_BWTREE`: build a benchmarker with OpenBw-Tree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_MASSTREE`: build a benchmarker with Masstree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_YAKUSHIMA`: build a benchmarker with yakushima if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_ART_OLC`: build a benchmarker with OLC based ART if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_HYDRALIST`: build a benchmarker with HydraList if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_ALEX_OLC`: build a benchmarker with OLC based ALEX if `ON` (default: `OFF`).

### Build Options for Unit Testing

- `INDEX_BENCH_BUILD_TESTS`: build unit tests if `ON` (default: `OFF`).
- `INDEX_BENCH_TEST_THREAD_NUM`: the maximum number of threads to perform unit tests (default `8`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DINDEX_BENCH_BUILD_TESTS=ON ..
make -j
ctest -C Release
```

## Usage

The following command displays available CLI options:

```bash
./index_bench --helpshort
./bulkload_bench --helpshort
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
