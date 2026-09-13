#!/bin/bash
# Ricochet Server health check.
#
# Asks the server's operator HTTP surface rather than inferring its state
# from the outside. Exit codes follow the Nagios convention: 0 healthy,
# 1 warning (up but not ready), 2 critical (not responding), so the script
# drops into cron, Nagios, or a load balancer's external check unchanged.
#
# The operator surface is bound to loopback on port 9090 by default (the
# `ops` section of /etc/ricochet/config.yaml); set OPS to match if changed.

OPS="${OPS:-http://127.0.0.1:9090}"

# Liveness. A failure here means the process is gone or wedged. /healthz
# touches no dependency on purpose, so a database outage does not read as a
# dead process.
if ! curl -sf --max-time 5 "$OPS/healthz" >/dev/null; then
    echo "CRITICAL: Ricochet not responding at $OPS"
    exit 2
fi

# Readiness. 503 means the process is up but cannot serve, usually because
# PostgreSQL is unreachable or the server is shutting down. The body says
# which check failed.
if ! curl -sf --max-time 5 "$OPS/readyz" >/dev/null; then
    echo "WARNING: Ricochet is up but not ready"
    curl -s --max-time 5 "$OPS/readyz"
    echo
    exit 1
fi

echo "OK: Ricochet is healthy"
exit 0
