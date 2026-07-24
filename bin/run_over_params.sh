#!/bin/bash
set -eu

################################################################################
# Documents
################################################################################

BENCH_BIN=""
CONFIG_ENV=""
NUMA_NODES=""
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <bench_bin> <config>
Description:
  Run benchmark to measure throughput/latency with workloads based on the
  specified parameters. All the benchmark results are output in CSV format.
Arguments:
  <bench_bin>: A path to a binary file for benchmarking.
  <config>: A path to a configuration file for benchmarking.
Options:
  -o: Set a file path to write the benchmarking results.
  -n: Only execute benchmark on the CPUs of nodes. See "man numactl" for details.
  -h: Show this message and exit.
EOS
  exit 1
}

################################################################################
# Parse options
################################################################################

while getopts o:n:h OPT
do
  case ${OPT} in
    o) OUTPUT_FILE="${OPTARG}"
      ;;
    n) NUMA_NODES=${OPTARG}
      ;;
    h) usage
      ;;
    \?) usage
      ;;
  esac
done
shift $((${OPTIND} - 1))

################################################################################
# Parse arguments
################################################################################

if [ ${#} != 2 ]; then
  usage
fi

BENCH_BIN=${1}
CONFIG_ENV=${2}
if [ -n "${NUMA_NODES}" ]; then
  BENCH_BIN="numactl -N ${NUMA_NODES} -m ${NUMA_NODES} ${BENCH_BIN}"
fi

if [ ! -f "${BENCH_BIN}" ]; then
  echo "There is no specified benchmark binary."
  exit 1
fi
if [ ! -f "${CONFIG_ENV}" ]; then
  echo "There is no specified configuration file."
  exit 1
fi

################################################################################
# Run benchmark
################################################################################

# run a benchmark program with the variety of parameters
source "${CONFIG_ENV}"
for IDX_SIZE in ${INDEX_SIZE_CANDIDATES}; do
  for W_RATIO in ${WRITE_RATIO_CANDIDATES}; do
    for SKEW in ${SKEW_CANDIDATES}; do
      for SCAN_SIZE in ${SCAN_SIZE_CANDIDATES}; do
        # prepare temporary paths
        TMP_RND="$(tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 8)"
        TMP_STR="$(date '+%Y%m%d_%H%M%S')-${TMP_RND}"
        TMP_OUT="/tmp/index_bench-${TMP_STR}.csv"
        TMP_WLD="/tmp/index_bench-${TMP_STR}.yaml"
        rm -f ${TMP_OUT} ${TMP_WLD}

        # create a temporary workload YAML
        R_RATIO=$(echo "1 - ${W_RATIO}" | bc | sed "s/^\./0./g")
        cat << EOF > ${TMP_WLD}
workload: zipf

operations:
  - ratios: { ${READ_OP}: ${R_RATIO}, ${WRITE_OP}: ${W_RATIO} }
    scan_size: ${SCAN_SIZE}
    per_thread: false
    skew_parameter: ${SKEW}
    duration: ${DURATION}

vary_hot_spot: false

initialization:
  use_all_cores: true
  use_bulkload: false

dataset:
  type: integer
  src:
    type: simulation
    path: null
  num: ${IDX_SIZE}
  has_locality: true
EOF

        # format and append the benchmarking results
        ${WORKSPACE_DIR}/bin/measure.sh \
          "${BENCH_BIN}" \
          "${CONFIG_ENV}" \
          "${TMP_WLD}" \
          >> "${TMP_OUT}"
        sed "s/^/${IDX_SIZE},${W_RATIO},${SKEW},${SCAN_SIZE},/g" "${TMP_OUT}"
        rm -f ${TMP_OUT} ${TMP_WLD}
      done
    done
  done
done
