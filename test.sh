#!/usr/bin/env bash
#
# Manual test runner for the production hardening changes:
#   1. Graceful shutdown      (workers/handler.py)
#   2. Built-in /healthz      (server.py)
#   3. request_id tracing     (protocol/cprotocol.pyx)
#
# Usage:
#   ./test.sh                 # unit tests + live integration tests
#   ./test.sh --unit          # only the in-process unit tests (no sockets)
#   ./test.sh --build         # rebuild Cython extensions before testing
#
# Env:
#   PYTHON        python interpreter to use (default: python3)
#   VIBORA_TEST_PORT  base port (integration tests use port/+1/+2)
set -u

cd "$(dirname "$0")"
ROOT="$(pwd)"
PYTHON="${PYTHON:-python3}"
UNIT_ONLY=0
DO_BUILD=0

for arg in "$@"; do
    case "$arg" in
        --unit)  UNIT_ONLY=1 ;;
        --build) DO_BUILD=1 ;;
        *) echo "unknown argument: $arg"; exit 2 ;;
    esac
done

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'; END='\033[0m'
pass() { echo -e "${GREEN}[PASS]${END} $1"; }
info() { echo -e "${YELLOW}[INFO]${END} $1"; }
fail() { echo -e "${RED}[FAIL]${END} $1"; }

FAILED=0
run_case() {
    local name="$1"; shift
    info "== $name =="
    if "$PYTHON" "$@"; then
        pass "$name"
    else
        fail "$name"
        FAILED=1
    fi
}

if [ "$DO_BUILD" -eq 1 ]; then
    info "Rebuilding Cython extensions..."
    "$PYTHON" build.py || { fail "build failed"; exit 1; }
    pass "build"
fi

if ! "$PYTHON" -c "import tests.manual._compat; import vibora.protocol.cprotocol" 2>/dev/null; then
    fail "compiled vibora extensions not found. Run: PYTHON=<interpreter> $0 --build"
    exit 1
fi

export VIBORA_TEST_PORT="${VIBORA_TEST_PORT:-8097}"

# 1. Unit tests (no network required).
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
run_case "graceful shutdown: cleanup registry unit tests" \
    tests/manual/test_graceful_shutdown.py

if [ "$UNIT_ONLY" -eq 1 ]; then
    [ "$FAILED" -eq 0 ] && echo -e "${GREEN}ALL UNIT TESTS PASSED${END}" || echo -e "${RED}UNIT TESTS FAILED${END}"
    exit "$FAILED"
fi

# 2. Live integration tests (fork real workers, open real TCP sockets).
export VIBORA_TEST_STATE="$(mktemp -d /tmp/vibora_manual_state.XXXXXX)"

VIBORA_TEST_PORT="$((VIBORA_TEST_PORT))" \
    run_case "live: graceful shutdown (SIGTERM + callbacks + task cancel)" \
    tests/manual/test_graceful_shutdown_live.py

VIBORA_TEST_PORT="$((VIBORA_TEST_PORT + 1))" \
    run_case "live: /healthz endpoint (workers, connections, memory)" \
    tests/manual/test_healthz.py

VIBORA_TEST_PORT="$((VIBORA_TEST_PORT + 2))" \
    run_case "live: request_id tracing (UUID, propagation, logs)" \
    tests/manual/test_request_id.py

if [ "$FAILED" -eq 0 ]; then
    echo -e "${GREEN}ALL MANUAL TESTS PASSED${END}"
else
    echo -e "${RED}SOME MANUAL TESTS FAILED${END}"
fi
exit "$FAILED"
