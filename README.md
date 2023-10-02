# Index Benchmark

[![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/index-benchmark/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/index-benchmark/actions/workflows/unit_tests.yaml)

- [Build](#build)
    - [Prerequisites](#prerequisites)
    - [Build Options](#build-options)
    - [Build and Run Unit Tests](#build-and-run-unit-tests)
- [Usage](#usage)
- [Acknowledgments](#acknowledgments)

## Build

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake libgflags-dev
cd <path_to_your_workspace>
git clone --recursive https://github.com/dbgroup-nagoya-u/index-benchmark.git
```

#### Using An Efficient Memory Allocator

We optionally use [mimalloc](https://github.com/microsoft/mimalloc) for efficient memory allocation. If you want to use this feature in benchmarking, please [install mimalloc](https://github.com/microsoft/mimalloc#macos-linux-bsd-etc) in advance.

#### Preparation for Existing Implementations

Some existing indexes use [Intel OneAPI Base Toolkit](https://www.intel.com/content/www/us/en/developer/tools/oneapi/base-toolkit.html) (i.e., Threading Building Blocks), so please [prepare Intel OneAPI Base Toolkit](https://www.intel.com/content/www/us/en/developer/tools/oneapi/base-toolkit-download.html?operatingsystem=linux&distributions=aptpackagemanager) in advance if you want to compare the state-of-the-art indexes. If you prefer to install oneTBB separately, you can use the following targets instead of `intel-basekit`:

```bash
sudo apt install intel-oneapi-tbb-devel
```

You also need the following packages for Yakushima (`libgoogle-glog-dev`) and HydraList (`libnuma-dev`).

```bash
sudo apt install libgoogle-glog-dev libnuma-dev
```

### Build Options

#### Utility Options

- `INDEX_BENCH_BUILD_LONG_KEYS`: build keys with sizes of 16/32/64/128 bytes if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_OPTIMIZED_B_TREES`: build the optimized B+trees for fixed-length keys if `ON` (default: `OFF`).

#### Memory Allocation

- `INDEX_BENCH_OVERRIDE_MIMALLOC`: override entire memory allocation with mimalloc if `ON` (default: `OFF`).

#### Optional Benchmarking Targets

- `INDEX_BENCH_BUILD_BTREE_OLC`: build a benchmarker with OLC based B+tree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_BTREE_OPTIQL`: build a benchmarker with OptiQL based B+tree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_OPEN_BWTREE`: build a benchmarker with OpenBw-Tree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_MASSTREE`: build a benchmarker with Masstree if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_YAKUSHIMA`: build a benchmarker with yakushima if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_ART_OLC`: build a benchmarker with OLC based ART if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_HYDRALIST`: build a benchmarker with HydraList if `ON` (default: `OFF`).
- `INDEX_BENCH_BUILD_ALEX_OLC`: build a benchmarker with OLC based ALEX if `ON` (default: `OFF`).

#### Build Options for Unit Testing

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
./build/index_bench --helpshort
```

The following command displays available indexes:

```bash
./build/index_bench --helpon=index
```

For example, if you want to measure Bw-tree's throughput in the YCSB-C workload with 8 threads, execute the following command:

```bash
./build/index_bench --bw --num-thread 8 --workload "workload/ycsb_c.json"
```

If you want to measure latency, use `--throughput=f` flag:

```bash
./build/index_bench --bw --num-thread 8 --workload "workload/ycsb_c.json" --throughput=f
```

We prepare scripts in `bin` directory to measure performance with a variety of parameters. You can set parameters for benchmarking by `config/bench.env`.

## Acknowledgments

This work is based on results from project JPNP16007 commissioned by the New Energy and Industrial Technology Development Organization (NEDO), and it was supported partially by KAKENHI (JP20K19804, JP21H03555, and JP22H03594). We also thank Takayuki Tanabe and our students for contributing to implementations.
