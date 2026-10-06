#!/usr/bin/env bash
# setup_elle.sh -- install the pinned Elle checker on the DRIVER HOST (Arch §10.3, §17).
#
# Elle itself is a library; the harness runs it through elle-cli (github.com/ligurio/elle-cli),
# which ships as a release zip containing a standalone jar. The jar is installed here as
# vendor/elle/elle-cli.jar, which is where resilience_tests/analysis/elle_checker.py looks.
# Without it, Elle-gated scenarios report elle_anomalies_count as NOT_MEASURED and fail --
# there is no fallback checker.

set -euo pipefail

ELLE_CLI_VERSION="${ELLE_CLI_VERSION:-0.1.11}"   # pinned; needs Java 21+
URL="https://github.com/ligurio/elle-cli/releases/download/${ELLE_CLI_VERSION}/elle-cli-bin-${ELLE_CLI_VERSION}.zip"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ELLE_JAR="${SCRIPT_DIR}/elle-cli.jar"

if ! command -v java >/dev/null 2>&1; then
    echo "java not found: install a JRE 21+ (e.g. sudo apt-get install -y openjdk-21-jre-headless)" >&2
    exit 1
fi
JAVA_MAJOR="$(java -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)"
if [[ -z "${JAVA_MAJOR}" || "${JAVA_MAJOR}" -lt 21 ]]; then
    echo "elle-cli ${ELLE_CLI_VERSION} needs Java 21+, found: $(java -version 2>&1 | head -1)" >&2
    exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
echo "downloading ${URL}"
curl -fsSL -o "${TMP}/elle-cli.zip" "${URL}"
unzip -q "${TMP}/elle-cli.zip" -d "${TMP}/unpacked"
JAR="$(find "${TMP}/unpacked" -name '*standalone*.jar' | head -1)"
if [[ -z "${JAR}" ]]; then
    echo "no standalone jar inside the release zip; contents:" >&2
    find "${TMP}/unpacked" -type f >&2
    exit 1
fi
install -m 0644 "${JAR}" "${ELLE_JAR}"
if ! command -v dot >/dev/null 2>&1; then
    echo "note: graphviz (dot) is not installed. Verdicts are unaffected, but anomaly"
    echo "      explanations and cycle plots are only written with it (sudo apt-get install -y graphviz)."
fi
echo "installed $(basename "${JAR}") as ${ELLE_JAR}"
echo "sha256: $(sha256sum "${ELLE_JAR}" | cut -d' ' -f1)  -- record it, so a changed jar is noticed"

# smoke test: a two-transaction history Elle must accept
cat > "${TMP}/smoke.edn" <<'EDN'
{:index 0, :type :invoke, :f :txn, :process 0, :value [[:append 1 1] [:r 1 nil]]}
{:index 1, :type :ok, :f :txn, :process 0, :value [[:append 1 1] [:r 1 [1]]]}
{:index 2, :type :invoke, :f :txn, :process 1, :value [[:append 1 2] [:r 1 nil]]}
{:index 3, :type :ok, :f :txn, :process 1, :value [[:append 1 2] [:r 1 [1 2]]]}
EDN
java -jar "${ELLE_JAR}" --model list-append --consistency-models serializable "${TMP}/smoke.edn"
