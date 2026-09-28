#!/usr/bin/env bash
# setup_elle.sh -- Setup Jepsen's Elle consistency checker for the resilience harness (Arch §10.3, §17).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "=== Setting up Elle Checker in ${SCRIPT_DIR} ==="

# 1. Check / install Java
if ! command -v java >/dev/null 2>&1; then
    echo "Java not found. Please install JRE (e.g. sudo apt-get update && sudo apt-get install -y default-jre-headless)"
    if command -v sudo >/dev/null 2>&1; then
        echo "Attempting to install default-jre-headless via sudo..."
        sudo apt-get update && sudo apt-get install -y default-jre-headless || true
    fi
else
    echo "Java is installed: $(java -version 2>&1 | head -n 1)"
fi

# 2. Download / setup pinned elle-cli standalone jar if available or create clojure runner
ELLE_JAR="${SCRIPT_DIR}/elle-cli.jar"

if [[ ! -f "${ELLE_JAR}" ]]; then
    echo "Downloading pinned elle-cli jar..."
    # If GitHub release is reachable:
    curl -fsSL -o "${ELLE_JAR}" "https://github.com/jepsen-io/elle/releases/download/0.1.6/elle-cli.jar" 2>/dev/null || {
        echo "Note: Offline/direct download unavailable. The harness includes an in-process verifier"
        echo "and will seamlessly use elle-cli.jar whenever placed in ${ELLE_JAR}."
    }
fi

echo "=== Elle setup script completed ==="
