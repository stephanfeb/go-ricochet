#!/usr/bin/env bash
# Compiles the Go snippets in README.md's "Client Library" section from a
# module outside this repository, which is the only way to prove that the
# public packages (pkg/client, pkg/wire) are usable by an external consumer
# and that no snippet quietly depends on an internal package.
#
# Each fenced ```go block becomes one function. Identifiers the snippets use
# without declaring (ctx, peer IDs, a message) are declared in a preamble,
# and every name a snippet declares is marked used so the compiler checks
# the calls rather than the bookkeeping.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

python3 - "$REPO/README.md" "$WORK/examples.go" <<'PY'
import re, sys
readme, out = sys.argv[1], sys.argv[2]
text = open(readme).read()
start = text.index('## Client Library')
end = text.index('\n## ', start + 1)
section = text[start:end]
blocks = re.findall(r'```go\n(.*?)```', section, re.S)

preamble = '''package main

import (
    "context"
    "fmt"
    "time"

    "github.com/libp2p/go-libp2p"
    "github.com/libp2p/go-libp2p/core/peer"
    client "github.com/twostack/go-ricochet/pkg/client"
    "github.com/twostack/go-ricochet/pkg/wire"
)

var (
    ctx             = context.Background()
    recipientID     peer.ID
    serverPeerID    peer.ID
    colleaguePeerID peer.ID
    payload         []byte
    msg             = wire.NewMessage("", "", nil)
    cl              *client.Client
)

var _ = fmt.Sprintf
var _ = time.Second
var _ = libp2p.New

func main() {}
'''
funcs = []
for i, block in enumerate(blocks):
    body = re.sub(r'^import \((?:.|\n)*?\)\n', '', block, flags=re.M)
    # Mark each declared name used right where it is declared, so names
    # declared inside a nested block are still in scope.
    lines = []
    for line in body.split('\n'):
        lines.append(line)
        m = re.match(r'^(\s*)([\w, ]+?)\s*:=', line)
        if m:
            indent = m.group(1)
            names = [n.strip() for n in m.group(2).split(',')]
            names = [n for n in names if n and n != '_']
            # A multi-line statement ends at the matching close; defer the
            # use until the statement's last line so it is not mid-call.
            lines.append(indent + '__USE__ ' + ' '.join(names))
    text = '\n'.join(lines)
    # Move each __USE__ marker past the statement it follows: a statement
    # continues while parentheses are unbalanced.
    out_lines, pending = [], None
    depth = 0
    for line in text.split('\n'):
        if line.strip().startswith('__USE__'):
            pending = line
            continue
        out_lines.append(line)
        depth += line.count('(') - line.count(')')
        if pending is not None and depth == 0:
            indent, _, names = pending.partition('__USE__')
            out_lines.extend(f'{indent}_ = {n}' for n in names.split())
            pending = None
    funcs.append(f'func example{i}() {{\n' + '\n'.join(out_lines) + '\n}\n')
open(out, 'w').write(preamble + '\n' + '\n'.join(funcs))
print(f'{len(blocks)} snippets extracted')
PY

cd "$WORK"
cat > go.mod <<GOMOD
module example.com/readme-check

go $(sed -n 's/^go \(.*\)$/\1/p' "$REPO/go.mod")

require github.com/twostack/go-ricochet v0.0.0
replace github.com/twostack/go-ricochet => $REPO
GOMOD
# The example module has to resolve the same versions as the repo; copying
# its go.sum is quicker and quieter than letting tidy rediscover them.
cp "$REPO/go.sum" go.sum
GOFLAGS=-mod=mod go mod tidy >/dev/null 2>&1
gofmt -w examples.go
go vet ./...
echo "README client examples compile from an external module"
