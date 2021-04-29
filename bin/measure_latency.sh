#!/bin/bash
set -ue

########################################################################################
# Documents
########################################################################################

BENCH_BIN=""
NUMA_NODES=""
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <path_to_bench_bin> 1> results.csv 2> error.log
Description:
  Run benchmark to measure latency. All the benchmark results are output in CSV
  format. Note that all the benchmark settings are set by "config/bench.env".
Arguments:
  <path_to_bench_bin>: A path to the binary file for benchmarking.
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

if [ ${#} != 1 ]; then
  usage
fi

BENCH_BIN=${1}
if [ -n "${NUMA_NODES}" ]; then
  BENCH_BIN="numactl -N ${NUMA_NODES} -m ${NUMA_NODES} ${BENCH_BIN}"
fi

########################################################################################
# Run benchmark
########################################################################################

source ${WORKSPACE_DIR}/config/bench.env

for SKEW_PARAMETER in ${SKEW_CANDIDATES}; do
  for THREAD_NUM in ${THREAD_CANDIDATES}; do
    for IMPL in ${IMPL_CANDIDATES}; do
      if [ ${IMPL} == 0 ]; then
        IMPL_ARGS="--open_bw=t"
      else
        continue
      fi
      for LOOP in {1..${BENCH_REPEAT_COUNT}}; do
        echo -n "${SKEW_PARAMETER},${IMPL},${THREAD_NUM},"
        ${BENCH_BIN} \
          --csv --throughput=t ${IMPL_ARGS} --num_exec ${OPERATION_COUNT} \
          --skew_parameter ${SKEW_PARAMETER} --num_thread ${THREAD_NUM}
        echo ""
      done
    done
  done
done
