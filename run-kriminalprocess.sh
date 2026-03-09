#!/bin/sh

set -eu

SCRIPT_DIR=$(
  CDPATH= cd -- "$(dirname -- "$0")" && pwd
)

if [ "$#" -eq 0 ]; then
  set -- "https://tis.ta.gov.lv/court.jm.gov.lv/stat/html/index_$(date +%Y%m).html"
fi

cd "$SCRIPT_DIR"
node scripts/merge-kriminalprocess.mjs "$@"
