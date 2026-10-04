#!/bin/sh
# Configures this container as a NAT router for INTERNAL_SUBNET.
#
# NAT_TYPE=cone       keeps a private host's source port where it can, and
#                     lets a packet in only from an address the host has
#                     sent to (a port-restricted cone NAT, the common home
#                     router). Hole punching works through it.
# NAT_TYPE=symmetric  picks a random public port for every new destination,
#                     so the port a peer learns from the relay is not the
#                     one its punch uses: hole punching fails, and the chat
#                     stays on the relay.
set -e

# Docker does not fix the interface order, so find the internal interface
# by its subnet (a /24) and take the other one as the external interface.
PREFIX="${INTERNAL_SUBNET%.*}."
INT=$(ip -o -4 addr show | awk -v p="$PREFIX" 'index($4, p) == 1 {print $2; exit}')
EXT=$(ip -o -4 addr show | awk -v i="$INT" '$2 ~ /^eth/ && $2 != i {print $2; exit}')
echo "NAT ($NAT_TYPE): $INT ($INTERNAL_SUBNET) -> $EXT"

# Like a home router, drop unsolicited packets addressed to the router
# itself on its WAN side. This matters for hole punching: a packet that is
# accepted here creates a conntrack entry for its address pair, and the NAT
# then gives an outgoing punch for the same pair a different port. A dropped
# packet leaves no entry.
iptables -A INPUT -i "$EXT" -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A INPUT -i "$EXT" -j DROP

iptables -A FORWARD -i "$INT" -o "$EXT" -j ACCEPT
iptables -A FORWARD -i "$EXT" -o "$INT" -m state --state ESTABLISHED,RELATED -j ACCEPT
iptables -A FORWARD -i "$EXT" -o "$INT" -j DROP
case "$NAT_TYPE" in
    cone)      iptables -t nat -A POSTROUTING -s "$INTERNAL_SUBNET" -o "$EXT" -j MASQUERADE ;;
    symmetric) iptables -t nat -A POSTROUTING -s "$INTERNAL_SUBNET" -o "$EXT" -j MASQUERADE --random ;;
    *)         echo "Unknown NAT_TYPE: $NAT_TYPE"; exit 1 ;;
esac

# A real internet path has latency. Without it, one peer's punch can reach
# the other NAT before that NAT has sent its own punch out, and the NAT then
# maps the outgoing punch to a new port.
tc qdisc add dev "$EXT" root netem delay "${WAN_DELAY:-50ms}" 2>/dev/null \
    || echo "No netem in this kernel; running without WAN delay"

echo "NAT ready"
trap 'exit 0' TERM INT
while true; do sleep 3600 & wait $!; done
