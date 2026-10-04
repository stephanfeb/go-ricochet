#!/bin/sh
# Starts the Ricochet server, and writes the address that clients dial to
# /shared/server.addr once the server prints its peer ID.
#
# The identity key lives in the data directory (/data), which is a volume, so
# the server keeps its peer ID, and so its address, across restarts.
set -e

PORT=${PORT:-55223}
rm -f /shared/server.addr

ricochet --development \
    --port "$PORT" \
    --data-dir /data \
    --pg-host "$PG_HOST" --pg-database "$PG_DB" --pg-username "$PG_USER" \
    --pg-sslmode disable \
    --external-addrs "/ip4/$EXTERNAL_IP/udp/$PORT/udx" 2>&1 |
while IFS= read -r line; do
    echo "$line"
    case "$line" in
        *"Peer ID: "*)
            echo "/ip4/$EXTERNAL_IP/udp/$PORT/udx/p2p/${line##*Peer ID: }" > /shared/server.addr
            ;;
    esac
done
