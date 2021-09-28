#!/bin/bash
set -ue

########################################################################################
# Documents
########################################################################################

BENCH_BIN=""
WORKLOAD=""
NUMA_NODES=""
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <bench_bin> <workload_json> 1> results.csv 2> error.log
Description:
  Run benchmark to measure throughput. All the benchmark results are output in CSV
  format. Note that all the benchmark settings are set by "config/bench.env".
Arguments:
  <bench_bin>: A path to the binary file for benchmarking.
  <workload_json>: A path to the workload JSON file.
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

if [ ${#} != 2 ]; then
  usage
fi

BENCH_BIN=${1}
WORKLOAD=${2}
if [ -n "${NUMA_NODES}" ]; then
  BENCH_BIN="numactl -N ${NUMA_NODES} -m ${NUMA_NODES} ${BENCH_BIN}"
fi

########################################################################################
# Run benchmark
########################################################################################

source ${WORKSPACE_DIR}/config/bench.env

for IMPL in ${IMPL_CANDIDATES}; do
  if [ ${IMPL} == 0 ]; then
    IMPL_ARGS="--bz=t --open-bw=f --mass=f --p=f"
  elif [ ${IMPL} == 1 ]; then
    IMPL_ARGS="--bz=f --open-bw=t --mass=f --p=f"
  elif [ ${IMPL} == 2 ]; then
    IMPL_ARGS="--bz=f --open-bw=f --mass=t --p=f"
  else
    continue
  fi
  for SKEW_PARAMETER in ${SKEW_CANDIDATES}; do
    for THREAD_NUM in ${THREAD_CANDIDATES}; do
      for LOOP in `seq ${BENCH_REPEAT_COUNT}`; do
        echo -n "${SKEW_PARAMETER},${IMPL},${THREAD_NUM},"
        ${BENCH_BIN} ${IMPL_ARGS} \
          --csv --throughput=t --workload "${WORKLOAD}" \
          --num-exec ${OPERATION_COUNT} --num-key ${TOTAL_KEY_NUM} \
          --num-init-insert ${INIT_INSERT_NUM} --num-init-thread ${INIT_THREAD_NUM} \
          --skew-parameter ${SKEW_PARAMETER} --num-thread ${THREAD_NUM}
        echo ""
      done
    done
  done
done
