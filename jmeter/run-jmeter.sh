#!/bin/bash

COURSE_DIR="${HOME}/Documents/BCIT - CIT/Falls 2026/ACIT4855_2.0_/project_defence1/jmeter"

rm -rf "${COURSE_DIR}/results.jtl"
rm -rf "${COURSE_DIR}/report"

echo "${1:-100} threads, ${2:-10} loops"

jmeter \
  -t "${COURSE_DIR}/lab2_remake.jmx" \
  -Jthreads="${1:-10}" \
  -Jloops="${2:-10}" \
  -n -l "${COURSE_DIR}/results.jtl" \
  -e -o "${COURSE_DIR}/report"