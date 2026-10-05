#!/usr/bin/env bash
# run_elle.sh -- re-check a run's history.edn by hand with the pinned Elle checker.
#   vendor/elle/run_elle.sh <run-dir>/history.edn [consistency-model]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HISTORY_FILE="${1:?usage: $0 <path/to/history.edn> [consistency-model]}"
MODEL="${2:-serializable}"

if [[ ! -f "${SCRIPT_DIR}/elle-cli.jar" ]]; then
    echo "${SCRIPT_DIR}/elle-cli.jar is missing: run ${SCRIPT_DIR}/setup_elle.sh" >&2
    exit 1
fi
OUT="$(dirname "${HISTORY_FILE}")/elle"
mkdir -p "${OUT}"
java -jar "${SCRIPT_DIR}/elle-cli.jar" --model list-append --consistency-models "${MODEL}" \
    --directory "${OUT}" "${HISTORY_FILE}"
