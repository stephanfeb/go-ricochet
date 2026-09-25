#!/bin/bash
# Build the Ricochet Server Debian packages on a non-Linux host (macOS),
# inside the Docker build environment. On Linux, scripts/package-deb.sh runs
# directly; releases are built by .github/workflows/release.yml.
#
#   ./build-deb.sh                          # version from the latest tag, amd64
#   VERSION=1.0.1 ARCHES="amd64 arm64" ./build-deb.sh

set -euo pipefail

cd "$(dirname "$0")"

if [ -z "${VERSION:-}" ]; then
    VERSION="$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')"
    VERSION="${VERSION:-0.0.0~dev}"
fi
ARCHES="${ARCHES:-amd64}"
# The package version, as scripts/package-deb.sh writes it: 1.0.0-rc1 -> 1.0.0~rc1.
DEB_VERSION="$(printf %s "$VERSION" | tr - "~")"

echo "Ricochet Server package build: version ${VERSION}, arch ${ARCHES}"

docker compose -f docker-compose.build.yml build
docker compose -f docker-compose.build.yml run --rm \
    -e VERSION="${VERSION}" \
    -e ARCHES="${ARCHES}" \
    -e HOST_USER_ID="$(id -u)" \
    -e HOST_GROUP_ID="$(id -g)" \
    builder \
    /bin/bash docker-build.sh

echo ""
ls -lh build/dist/ricochet-server_"${DEB_VERSION}"_*.deb
echo ""
echo "Install on Debian or Ubuntu:"
echo "  sudo apt install ./build/dist/ricochet-server_${DEB_VERSION}_amd64.deb"
