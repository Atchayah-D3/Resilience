#!/usr/bin/env bash
# run_elle.sh -- Execute Elle consistency check on history.edn

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HISTORY_FILE="${1:-history.edn}"

if [[ ! -f "${HISTORY_FILE}" ]]; then
    echo "Usage: $0 <path/to/history.edn>"
    exit 1
fi

if [[ -f "${SCRIPT_DIR}/elle-cli.jar" ]] && command -v java >/dev/null 2>&1; then
    java -jar "${SCRIPT_DIR}/elle-cli.jar" list-append "${HISTORY_FILE}"
else
    echo "Running in-process fallback checker..."
    python3 -c "
from pathlib import Path
from resilience_tests.analysis.elle_checker import ElleChecker
res = ElleChecker.check(Path('${HISTORY_FILE}'))
print(f'Valid: {res.valid}, Anomalies: {res.anomalies_count}, Checker: {res.checker}')
if res.anomalies:
    print('Anomalies detail:', res.anomalies)
"
fi
