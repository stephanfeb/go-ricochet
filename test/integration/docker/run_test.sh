#!/bin/bash
#
# Store Integration Test Runner
#
# Builds and runs Docker-based store integration tests with a Go server
# and two Dart clients exercising all store protocols (SDA, SCA, SFA, MSA/MAA/MMA).
#
# Usage: ./run_test.sh
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/compose/docker-compose.yml"
GO_RICOCHET_DIR="${SCRIPT_DIR}/../../.."

# Check for pre-built .deb package
DEB_PATH="${GO_RICOCHET_DIR}/build/dist/ricochet-server_1.0.0.deb"
if [ ! -f "$DEB_PATH" ]; then
    echo "ERROR: Pre-built .deb not found at ${DEB_PATH}"
    echo "Build it first:  cd ${GO_RICOCHET_DIR} && ./docker-build.sh"
    exit 1
fi

echo "============================================"
echo "  Store Integration Tests"
echo "============================================"
echo ""
echo "Compose file: ${COMPOSE_FILE}"
echo "Server .deb:  ${DEB_PATH}"
echo ""

# Clean up any leftover containers from previous runs
echo "Cleaning up previous runs..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

# Build containers
echo "Building containers..."
docker compose -f "$COMPOSE_FILE" build || {
    echo "ERROR: Docker build failed"
    exit 1
}

# Start all services in the background
echo "Starting containers..."
docker compose -f "$COMPOSE_FILE" up -d

# Wait for both clients to finish (they exit when done)
echo "Waiting for test clients to complete..."
echo ""

CLIENT_A_EXIT=0
CLIENT_B_EXIT=0

docker wait store-test-client-a || CLIENT_A_EXIT=$?
echo "Client A (primary) exited with code: $CLIENT_A_EXIT"

docker wait store-test-client-b || CLIENT_B_EXIT=$?
echo "Client B (secondary) exited with code: $CLIENT_B_EXIT"

echo ""
echo "============================================"
echo "  Client A (primary) output:"
echo "============================================"
docker logs store-test-client-a 2>&1 || true

echo ""
echo "============================================"
echo "  Client B (secondary) output:"
echo "============================================"
docker logs store-test-client-b 2>&1 || true

echo ""
echo "============================================"

EXIT_CODE=0
if [ $CLIENT_A_EXIT -ne 0 ] || [ $CLIENT_B_EXIT -ne 0 ]; then
    echo "  TESTS FAILED"
    echo "    Client A exit: $CLIENT_A_EXIT"
    echo "    Client B exit: $CLIENT_B_EXIT"
    EXIT_CODE=1
else
    echo "  ALL TESTS PASSED"
fi

echo "============================================"

# Cleanup
echo ""
echo "Cleaning up..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

exit $EXIT_CODE
