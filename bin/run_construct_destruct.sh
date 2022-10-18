#!/bin/bash
set -eu

SCRIPT=${1}
BENCH_BIN=${2}
CONFIG_ENV=${3}
WORKSPACE_DIR=$(cd $(dirname ${BASH_SOURCE:-${0}})/.. && pwd)

for WORKLOAD in $(/usr/bin/ls "${WORKSPACE_DIR}/workload" | /usr/bin/grep -E '.*_(insert|delete).json'); do
  WORKLOAD_NAME=$(echo ${WORKLOAD} | sed "s/.json//g")
  OUTFILE="${WORKSPACE_DIR}/out/${WORKLOAD_NAME}.csv"

  ${SCRIPT} "${BENCH_BIN}" "${CONFIG_ENV}" "${WORKSPACE_DIR}/workload/${WORKLOAD}" \
      >> ${OUTFILE}
done
