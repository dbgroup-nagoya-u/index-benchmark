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
  Run MwCAS benchmark to measure latency. All the benchmark results are output in CSV
  format. Note that all the benchmark settings are set via "config/bench.env".
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

for SHARED_NUM in ${SHARED_CANDIDATES}; do
  for TARGET_NUM in ${TARGET_CANDIDATES}; do
    for THREAD_NUM in ${THREAD_CANDIDATES}; do
      for IMPL in ${IMPL_CANDIDATES}; do
        if [ ${IMPL} == 0 ]; then
          IMPL_ARGS="--ours=t --microsoft=f --single=f"
        elif [ ${IMPL} == 1 ]; then
          IMPL_ARGS="--ours=f --microsoft=t --single=f"
        else
          IMPL_ARGS="--ours=f --microsoft=f --single=t"
        fi
        for LOOP in {1..${BENCH_REPEAT_COUNT}}; do
          echo -n "${SHARED_NUM},${TARGET_NUM},${IMPL},${THREAD_NUM},"
          ${BENCH_BIN} \
            --csv --throughput=f --latency=t ${IMPL_ARGS} \
            --num_exec ${OPERATION_COUNT} --num_loop ${OPERATION_REPEAT_COUNT} \
            --num_target ${TARGET_NUM} --num_shared ${SHARED_NUM} \
            --num_thread ${THREAD_NUM}
          echo ""
        done
      done
    done
  done
done
