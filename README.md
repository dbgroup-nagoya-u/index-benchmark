# Simple MwCAS benchmark

![Unit Tests](https://github.com/dbgroup-nagoya-u/mwcas-benchmark/workflows/Unit%20Tests/badge.svg?branch=main)

## Build

### Prerequisites

Note: `libnuma-dev` is required to build Microsoft's PMwCAS.

```bash
sudo apt update && sudo apt install -y build-essential cmake libnuma-dev
```

### Build Options

List build options.

- `MWCAS_BENCH_MAX_FIELD_NUM`: The maximum number of target words of MwCAS (default: `8`).
- `MWCAS_BENCH_BUILD_TESTS`: build unit tests for this repository if `on` (default: `off`).

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DMWCAS_BENCH_BUILD_TESTS=on ..
make -j
ctest -C Release
```
