#!/bin/bash
set -ue

########################################################################################
# Documents
########################################################################################

BENCH_BIN=""
WORKLOAD=""
CONFIG_ENV=""
NUMA_NODES=""
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <bench_bin> <config> <workload_json> 1> results.csv 2> error.log
Description:
  Run benchmark to measure percentile latency. All the benchmark results are output in
  CSV format.
Arguments:
  <bench_bin>: A path to a binary file for benchmarking.
  <config>: A path to a configuration file for benchmarking.
  <workload_json>: A path to a workload JSON file.
Options:
  -n: Only execute benchmark on the CPUs of nodes. See "man numactl" for details.
  -h: Show this messsage and exit.
EOS
  exit 1
}

########################################################################################
# Parse options
########################################################################################

while getopts n:h OPT
do
  case ${OPT} in
    n) NUMA_NODES=${OPTARG}
      ;;
    h) usage
      ;;
    \?) usage
      ;;
  esac
done
shift $((${OPTIND} - 1))

########################################################################################
# Parse arguments
########################################################################################

if [ ${#} != 3 ]; then
  usage
fi

BENCH_BIN=${1}
CONFIG_ENV=${2}
WORKLOAD=${3}
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
if [ ! -f "${WORKLOAD}" ]; then
  echo "There is no specified workload JSON."
  exit 1
fi

########################################################################################
# Run benchmark
########################################################################################

source "${CONFIG_ENV}"

TMP_OUT="/tmp/index_bench-tmp_latency-$(id -un).csv"

for IMPL in ${IMPL_CANDIDATES}; do
  for KEY_SIZE in ${KEY_CANDIDATES}; do
    for THREAD_NUM in ${THREAD_CANDIDATES}; do
      for LOOP in `seq ${BENCH_REPEAT_COUNT}`; do
        rm -f "${TMP_OUT}"
        ${BENCH_BIN} \
          "--${IMPL}=t" \
          "--csv" \
          "--throughput=f" \
          "--workload" "${WORKLOAD}" \
          "--key-size" ${KEY_SIZE} \
          "--num-exec" ${OPERATION_COUNT} \
          "--num-thread" ${THREAD_NUM} \
          "--timeout" ${BENCH_TIME_OUT} \
          >> "${TMP_OUT}"
        sed "s/^/${IMPL},${KEY_SIZE},${THREAD_NUM},/g" "${TMP_OUT}"
      done
    done
  done
done

rm -f "${TMP_OUT}"
