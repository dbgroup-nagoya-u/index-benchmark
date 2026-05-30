#!/bin/bash
set -eu

usage() {
  cat 1>&2 << EOS
Usage:
  ${BASH_SOURCE:-${0}} <target_file> <delimiter> <column_id>
Description:
  Format a given CSV/TSV file for benchmarking.
Arguments:
  <enwiki_file>: A target Wikimedia's title dataset.
Example:
  ${BASH_SOURCE:-${0}} ./dataset/enwiki-yyyymmdd-all-titles '\t' 2
EOS
  exit 1
}

if [ $# -ne 3 ]; then
  usage
fi

DATASET_DIR=$(dirname ${1})
FILE=$(basename ${1})
DELIM=${2}
COLUMN=${3}
cd ${DATASET_DIR}

echo ${DELIM}

# remove useless data
if head -n 1 ${FILE} | grep -qP "${DELIM}"; then
  awk -i inplace -v col="${COLUMN}" -F"${DELIM}" '{print $col}' ${FILE}
  sed -i '1d' ${FILE}
fi

# sort all the rows in ascending order
if ! LC_ALL=C sort -c ${FILE} &> /dev/null; then
  LC_ALL=C sort -u ${FILE} > ${FILE}.tmp
  mv ${1}.tmp ${FILE}
fi
