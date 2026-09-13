#!/bin/bash
# Ricochet Server startup wrapper
# Loads /etc/ricochet/env and starts the server.

set -e

# Load environment variables if file exists
if [ -f /etc/ricochet/env ]; then
    # shellcheck disable=SC1091
    source /etc/ricochet/env
fi

# Ensure required environment variables are set
if [ -z "$DB_PASSWORD" ]; then
    echo "ERROR: DB_PASSWORD is not set"
    echo "Set it in /etc/ricochet/env: DB_PASSWORD=your_password"
    exit 1
fi

# Build external-addrs flag if EXTERNAL_IP is set.
# This is critical: without it, Identify only reports the VM's private IP,
# causing remote clients to lose the server's address from their peerstore.
EXTERNAL_ADDRS_FLAG=""
if [ -n "$EXTERNAL_IP" ]; then
    EXTERNAL_ADDRS_FLAG="--external-addrs /ip4/${EXTERNAL_IP}/udp/${LISTEN_PORT:-55223}/udx"
fi

# The password travels in the environment, not on argv: the command line of
# a running process is readable by every local user.
export RICOCHET_PG_PASSWORD="$DB_PASSWORD"

# Start the server with CLI flags
exec /opt/ricochet/ricochet_server \
    --production \
    --data-dir /var/lib/ricochet/sf_storage \
    --pg-host "${DB_HOST:-localhost}" \
    --pg-port "${DB_PORT:-5432}" \
    --pg-database "${DB_NAME:-ricochet}" \
    --pg-username "${DB_USER:-ricochet}" \
    --pg-sslmode "${DB_SSLMODE:-require}" \
    $EXTERNAL_ADDRS_FLAG
