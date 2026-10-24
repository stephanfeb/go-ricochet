#!/bin/sh
# Checks the server's relay with two clients behind separate NAT routers,
# in the mailbox example's Docker setup (examples/mailbox/docker):
#
#   - the relay carries 1 MiB on one relayed connection, where go-libp2p's
#     default limit would cut it at 128 KiB;
#   - the dialer reaches the listener through the server's relay, and a
#     hole punch then gives the two a direct connection.
#
# Exits non-zero when a check fails. NAT_TYPE=symmetric shows the other
# side: the punch fails and the connection stays on the relay.
set -e
cd "$(dirname "$0")"
c() {
    docker compose --project-directory ../../examples/mailbox/docker \
        -f ../../examples/mailbox/docker/compose.yml -f compose.yml "$@"
}
trap 'c down --remove-orphans >/dev/null 2>&1' EXIT

echo "== Starting PostgreSQL, the server, the NAT routers and the listener"
c up -d --build --wait server nat-a nat-b
c build -q dialer
c up -d listener
until LISTENER=$(c logs --no-log-prefix listener | awk '/^PEER /{print $2}') && [ -n "$LISTENER" ]; do
    sleep 1
done
c logs --no-log-prefix listener | grep "relay slot"

echo
echo "== Relay: 1 MiB through the relay, hole punching off"
c run --rm -T dialer bulk "$LISTENER"

echo
echo "== Hole punch: the dialer connects through the relay, then directly"
c run --rm -T dialer punch "$LISTENER"

echo
echo "All checks passed."
