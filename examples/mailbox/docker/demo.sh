#!/bin/sh
# Runs the mailbox scenario: alice writes to bob while bob is offline, bob
# collects the message later; then bob stays online and gets the next
# message pushed.
set -e
cd "$(dirname "$0")"
c() { docker compose "$@"; }

echo "== Starting PostgreSQL, the server and the two NAT routers"
c up -d --build --wait server nat-a nat-b
c build -q alice
until c exec -T server test -s /shared/server.addr; do sleep 1; done
echo "Server address: $(c exec -T server cat /shared/server.addr)"

ALICE=$(c run --rm -T alice id)
BOB=$(c run --rm -T bob id)
echo "alice is $ALICE"
echo "bob   is $BOB"

echo
echo "== bob is offline. alice sends him a message."
c run --rm -T alice send "$BOB" "Hi bob, this waited for you on the server."

echo
echo "== bob comes online and collects it."
c run --rm -T bob inbox

echo
echo "== bob stays online. alice sends again; the server pushes it to him."
c up -d bob
sleep 3
c run --rm -T alice send "$BOB" "And this one was pushed to you."
sleep 3
c logs --no-log-prefix bob | tail -3
c stop bob >/dev/null

echo
echo "Done. 'docker compose down' stops the rest; add -v to delete the data."
