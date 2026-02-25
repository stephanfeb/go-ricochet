#!/bin/bash
set -e

GO_SERVER_ADDR=${GO_SERVER_ADDR:-10.20.0.20}
GO_SERVER_PORT=${GO_SERVER_PORT:-55223}
CLIENT_ROLE=${CLIENT_ROLE:-primary}

echo "Store integration test client (role: ${CLIENT_ROLE})"
echo "Target server: ${GO_SERVER_ADDR}:${GO_SERVER_PORT}"

# Wait for Go server peer ID
echo "Waiting for Go server PeerID..."
TIMEOUT=60
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    if [ -f /shared/peer_id ] && [ -s /shared/peer_id ]; then
        GO_PEER_ID=$(cat /shared/peer_id)
        echo "Got server PeerID: ${GO_PEER_ID}"
        break
    fi
    sleep 1
    ELAPSED=$((ELAPSED + 1))
done

if [ -z "$GO_PEER_ID" ]; then
    echo "ERROR: Timed out waiting for Go server PeerID"
    exit 1
fi

TARGET="/ip4/${GO_SERVER_ADDR}/udp/${GO_SERVER_PORT}/udx/p2p/${GO_PEER_ID}"
echo "Connecting to: ${TARGET}"

exec /app/store_test_binary "$TARGET" "$CLIENT_ROLE"
