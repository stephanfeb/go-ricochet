#!/bin/bash
# Runs inside the build container (see docker-compose.build.yml); build-deb.sh
# on the host is the entry point. The packaging itself is scripts/package-deb.sh,
# the same script the release workflow runs on a Linux runner.

set -euo pipefail

VERSION="${VERSION:?VERSION is set by build-deb.sh}"
ARCHES="${ARCHES:-amd64}"

export HOME=/home/builder
export GOPATH=/home/builder/go
export GOCACHE=/home/builder/.cache/go-build
# The source is mounted read-only and owned by the host user, which git in
# the container refuses to read; the release workflow stamps VCS info instead.
export GOFLAGS="${GOFLAGS:-} -buildvcs=false"

sudo mkdir -p "$GOPATH/pkg/mod/cache" "$GOCACHE"
sudo chown -R builder:builder "$HOME" "$GOPATH" "$GOCACHE"

go version
for arch in $ARCHES; do
    VERSION="$VERSION" ARCH="$arch" OUT_DIR=build/dist scripts/package-deb.sh
done

# Hand the output back to the host user.
if [ -n "${HOST_USER_ID:-}" ] && [ -n "${HOST_GROUP_ID:-}" ]; then
    sudo chown -R "$HOST_USER_ID:$HOST_GROUP_ID" build
fi
