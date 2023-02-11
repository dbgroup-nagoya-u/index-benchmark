#!/bin/bash
set -eu

########################################################################################
# Documents
########################################################################################

BENCH_BIN=""
CONFIG_ENV=""
CRITERIA="throughput"
IS_THROUGHPUT="t"
NUMA_NODES=""
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <bench_bin> <config>
Description:
  Run benchmark to measure performance in index construction/destruction.
  All the benchmark results are output in CSV format.
Arguments:
  <bench_bin>: A path to a binary file for benchmarking.
  <config>: A path to a configuration file for benchmarking.
Options:
  -t: Use throughput as a performance criteria (default: true).
  -l: Use latency as a performance criteria (default: false). Note that this
      option will overwrites the "-t" option.
  -n: Only execute benchmark on the CPUs of nodes. See "man numactl" for details.
  -h: Show this messsage and exit.
EOS
  exit 1
}

########################################################################################
# Parse options
########################################################################################

while getopts tln:h OPT
do
  case ${OPT} in
    t) CRITERIA="throughput"; IS_THROUGHPUT="t"
      ;;
    l) CRITERIA="latency"; IS_THROUGHPUT="f"
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

########################################################################################
# Parse arguments
########################################################################################

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

########################################################################################
# Run benchmark
########################################################################################

# set an output file and temporary files
OUTPUT_FILE="${WORKSPACE_DIR}/out/construct_destruct_${CRITERIA}.csv"
TMP_OUTPUT="/tmp/index_benchmark-tmp_output-$(id -un).csv"
TMP_WORKLOAD="/tmp/index_benchmark-tmp_workload-$(id -un).json"

# create an output directory if not exist
mkdir -p "${WORKSPACE_DIR}/out"

# run a benchmark program with the variety of parameters
source "${CONFIG_ENV}"
for W_OPS in ${WRITE_OPS_CANDIDATES}; do
  for PARTITION in ${PARTITION_PATTERNS}; do
    # set access patterns according to partitioning
    if [ ${PARTITION} == "none" ]; then
      ACCESS_PATTERNS="random"
    else
      ACCESS_PATTERNS="ascending descending"
    fi
    for ACCESS in ${ACCESS_PATTERNS}; do
      for INDEX_SIZE in ${INDEX_SIZE_CANDIDATES}; do
        for IMPL in ${IMPL_CANDIDATES}; do
          for KEY_SIZE in ${KEY_CANDIDATES}; do
            for THREAD_NUM in ${THREAD_CANDIDATES}; do
              # remove an old output file
              rm -f ${TMP_OUTPUT}

              # compute the number of execution for each thread
              OPERATION_COUNT=$(echo "${INDEX_SIZE} / ${THREAD_NUM} + 1" | bc)
              ACT_INDEX_SIZE=$(echo "${OPERATION_COUNT} * ${THREAD_NUM}" | bc)

              # set the initial size of an index according to construct/destruct
              if [ ${W_OPS} == "write" ]; then
                # construction
                INITIAL_SIZE=0
              else
                # destruction
                INITIAL_SIZE=${ACT_INDEX_SIZE}
              fi

              # create a temporary workload JSON
              cat << EOF > ${TMP_WORKLOAD}
{
  "initialization": {
    "# of keys": ${INITIAL_SIZE},
    "use all cores": true
  },
  "workloads": [
    {
      "operation ratios": {
        "${W_OPS}": 1.0
      },
      "# of keys": ${ACT_INDEX_SIZE},
      "partitioning policy": "${PARTITION}",
      "access pattern": "${ACCESS}",
      "skew parameter": 0
    }
  ]
}
EOF

              # run a benchmark program
              for LOOP in `seq ${BENCH_REPEAT_COUNT}`; do
                ${BENCH_BIN} \
                  "--${IMPL}=t" \
                  "--csv" \
                  "--throughput=${IS_THROUGHPUT}" \
                  "--workload" "${TMP_WORKLOAD}" \
                  "--key-size" ${KEY_SIZE} \
                  "--num-exec" ${OPERATION_COUNT} \
                  "--num-thread" ${THREAD_NUM} \
                  >> ${TMP_OUTPUT}
              done

              # format and append the benchmarking results
              sed "s/^/${W_OPS},${PARTITION},${ACCESS},${INDEX_SIZE},${IMPL},${KEY_SIZE},${THREAD_NUM},/g" "${TMP_OUTPUT}" \
                >> ${OUTPUT_FILE}
            done
          done
        done
      done
    done
  done
done
