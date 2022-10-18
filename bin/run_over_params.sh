#!/bin/bash
set -eu

SCRIPT=${1}
BENCH_BIN=${2}
CONFIG_ENV=${3}
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

OUTPUT_FILE="${WORKSPACE_DIR}/out/over_params.csv"
TMP_OUTPUT="/tmp/index_benchmark-tmp_output-$(id -un).csv"
TMP_WORKLOAD="/tmp/index_benchmark-tmp_workload-$(id -un).json"

# run a benchmark program with the variety of parameters
source "${CONFIG_ENV}"
for INDEX_SIZE in ${INDEX_SIZE_CANDIDATES}; do
  for W_RATIO in ${WRITE_RATIO_CANDIDATES}; do
    for SKEW in ${SKEW_CANDIDATES}; do
      for SCAN_LENGTH in ${SCAN_LENGTH_CANDIDATES}; do
        # prepare parameters for workload
        R_RATIO=$(echo "1 - ${W_RATIO}" | bc | sed "s/^\./0./g")
        R_OPS=$(if [ ${SCAN_LENGTH} -eq 1 ]; then echo "read"; else echo "scan"; fi)

        # create a temporary workload JSON
        cat << EOF > ${TMP_WORKLOAD}
{
  "initialization": {
    "# of keys": ${INDEX_SIZE},
    "use all cores": true
  },
  "workloads": [
    {
      "operation ratios": {
        "${R_OPS}": ${R_RATIO},
        "write": ${W_RATIO}
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
        ${SCRIPT} "${BENCH_BIN}" "${CONFIG_ENV}" "${TMP_WORKLOAD}" \
          > ${TMP_OUTPUT}
        sed "s/^/${INDEX_SIZE},${W_RATIO},${SKEW},${SCAN_LENGTH},/g" "${TMP_OUTPUT}" \
          >> ${OUTPUT_FILE}
      done
    done
  done
done
