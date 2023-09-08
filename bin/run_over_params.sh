#!/bin/bash
set -u

########################################################################################
# Documents
########################################################################################

BENCH_BIN=""
CONFIG_ENV=""
CRITERIA="throughput"
IS_THROUGHPUT="t"
NUMA_NODES=""
TIMEOUT_SEC="3m"
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)
OUTPUT_FILE=""

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <bench_bin> <config>
Description:
  Run benchmark to measure performance in index construction/destruction. All the
  benchmark results are output in CSV format.
Arguments:
  <bench_bin>: A path to a binary file for benchmarking.
  <config>: A path to a configuration file for benchmarking.
Options:
  -t: Use throughput as a performance criteria (default: true).
  -l: Use latency as a performance criteria (default: false). Note that this option will
      overwrites the "-t" option.
  -o: Set a file path to write the benchmarking results.
  -n: Only execute benchmark on the CPUs of nodes. See "man numactl" for details.
  -r: Set a timeout value to prevent some indexes from going into infinite loops and
      retry benchmarking (default: 3m). See "man timeout" for details.
  -h: Show this messsage and exit.
EOS
  exit 1
}

########################################################################################
# Parse options
########################################################################################

while getopts tlo:n:r:h OPT
do
  case ${OPT} in
    t) CRITERIA="throughput"; IS_THROUGHPUT="t"
      ;;
    l) CRITERIA="latency"; IS_THROUGHPUT="f"
      ;;
    o) OUTPUT_FILE="${OPTARG}"
      ;;
    n) NUMA_NODES=${OPTARG}
      ;;
    r) TIMEOUT_SEC=${OPTARG}
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
if [ -z "${OUTPUT_FILE}" ]; then
  OUTPUT_FILE="${WORKSPACE_DIR}/out/over_params_${CRITERIA}.csv"
fi
TMP_OUTPUT="/tmp/index_benchmark-tmp_output-$(id -un).csv"
TMP_WORKLOAD="/tmp/index_benchmark-tmp_workload-$(id -un).json"

# create an output directory if not exist
mkdir -p "${WORKSPACE_DIR}/out"

# run a benchmark program with the variety of parameters
source "${CONFIG_ENV}"
for INDEX_SIZE in ${INDEX_SIZE_CANDIDATES}; do
  for IMPL in ${IMPL_CANDIDATES}; do
    for KEY_SIZE in ${KEY_CANDIDATES}; do
      for THREAD_NUM in ${THREAD_CANDIDATES}; do
        for W_RATIO in ${WRITE_RATIO_CANDIDATES}; do
          for SKEW in ${SKEW_CANDIDATES}; do
            for SCAN_LENGTH in ${SCAN_LENGTH_CANDIDATES}; do
              # remove an old output file
              rm -f ${TMP_OUTPUT}

              # prepare parameters for workload
              R_RATIO=$(echo "1 - ${W_RATIO}" | bc | sed "s/^\./0./g")
              R_OPS=$(if [ ${SCAN_LENGTH} -eq 1 ]; then echo "read"; else echo "scan"; fi)

              # create a temporary workload JSON
              cat << EOF > ${TMP_WORKLOAD}
{
  "initialization": {
    "# of keys": ${INDEX_SIZE},
    "use all cores": true,
    "use bulkload if possible": true
  },
  "workloads": [
    {
      "operation ratios": {
        "${R_OPS}": ${R_RATIO},
        "${WRITE_OPS}": ${W_RATIO}
      },
      "# of keys": ${INDEX_SIZE},
      "partitioning policy": "none",
      "access pattern": "random",
      "skew parameter": ${SKEW},
      "scan length": ${SCAN_LENGTH}
    }
  ]
}
EOF

              # run a benchmark program
              for LOOP in `seq ${BENCH_REPEAT_COUNT}`; do
                while : ; do
                  timeout ${TIMEOUT_SEC} \
                    ${BENCH_BIN} \
                    "--${IMPL}=t" \
                    "--csv" \
                    "--throughput=${IS_THROUGHPUT}" \
                    "--workload" "${TMP_WORKLOAD}" \
                    "--key-size" ${KEY_SIZE} \
                    "--num-exec" ${OPERATION_COUNT} \
                    "--num-thread" ${THREAD_NUM} \
                    "--timeout" ${BENCH_TIME_OUT} \
                    >> ${TMP_OUTPUT}
                  if [ ${?} -eq 0 ]; then
                    break
                  fi
                done
              done

              # format and append the benchmarking results
              sed "s/^/${INDEX_SIZE},${W_RATIO},${SKEW},${SCAN_LENGTH},${IMPL},${KEY_SIZE},${THREAD_NUM},/g" "${TMP_OUTPUT}" \
                >> ${OUTPUT_FILE}
            done
          done
        done
      done
    done
  done
done
