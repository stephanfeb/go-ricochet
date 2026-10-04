#!/bin/sh
# Routes the simulated internet through this home network's NAT router, as
# a home router would, waits for the server's address, then runs mailbox
# with the arguments given. The identity key is kept in /data, a volume.
set -e

ip route replace "$PUBLIC_NET" via "$NAT_GATEWAY"

while [ ! -s /shared/server.addr ]; do sleep 0.5; done
export RICOCHET_SERVER="$(cat /shared/server.addr)"

exec mailbox -key /data/peer.key "$@"
