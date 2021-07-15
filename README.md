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

- `INDEX_BENCH_BUILD_OPEN_BWTREE`: Build a benchmarker with OpenBw-Tree if `on` (default: `off`).
- `INDEX_BENCH_BUILD_PTREE`: Build a benchmarker with PTree if `on` (default: `off`).
    - Note: When you measure the performance of indexes other than PTree, turn off this option because PTree reserves threads for parallel writing.

### Build Options for Unit Testing

- `INDEX_BENCH_BUILD_TESTS`: Build unit tests if `on` (default: `off`).

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
