#!/bin/bash
# Build one Ricochet Server Debian package. Runs on Linux (the release
# workflow, or the container build-deb.sh starts on macOS); needs Go and
# dpkg-deb.
#
#   VERSION=1.0.0 ARCH=amd64 scripts/package-deb.sh
#
# VERSION is the release version without the leading v. A pre-release suffix
# (1.0.0-rc1) becomes 1.0.0~rc1 in the package version so dpkg orders it
# before 1.0.0. ARCH is a Debian and Go architecture: amd64 or arm64.
# The package is written to OUT_DIR (default build/dist).

set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${VERSION:?set VERSION, e.g. VERSION=1.0.0}"
ARCH="${ARCH:-amd64}"
OUT_DIR="${OUT_DIR:-build/dist}"

case "$ARCH" in
    amd64|arm64) ;;
    *) echo "unsupported ARCH: $ARCH (amd64 or arm64)" >&2; exit 1 ;;
esac

DEB_VERSION="$(printf %s "$VERSION" | tr - "~")"
PACKAGE="ricochet-server_${DEB_VERSION}_${ARCH}"
ROOT="build/package/${PACKAGE}"

rm -rf "$ROOT"
mkdir -p "$ROOT/DEBIAN" \
         "$ROOT/opt/ricochet" \
         "$ROOT/etc/ricochet" \
         "$ROOT/etc/supervisor/conf.d" \
         "$ROOT/var/lib/ricochet" \
         "$ROOT/var/log/ricochet"

echo "Compiling ricochet_server ${VERSION} for linux/${ARCH}"
CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o "$ROOT/opt/ricochet/ricochet_server" ./cmd/ricochet
chmod 755 "$ROOT/opt/ricochet/ricochet_server"

install -m 644 schema.sql              "$ROOT/opt/ricochet/"
install -m 755 deploy/run.sh           "$ROOT/opt/ricochet/"
install -m 755 deploy/health_check.sh  "$ROOT/opt/ricochet/"
install -m 644 config.example.yaml     "$ROOT/etc/ricochet/config.yaml.example"
install -m 644 deploy/config.yaml      "$ROOT/etc/ricochet/config.yaml"
install -m 644 deploy/env.example      "$ROOT/etc/ricochet/"
install -m 644 deploy/supervisor/ricochet.conf "$ROOT/etc/supervisor/conf.d/"

install -m 644 deploy/debian/conffiles "$ROOT/DEBIAN/"
install -m 755 deploy/debian/postinst  "$ROOT/DEBIAN/"
install -m 755 deploy/debian/prerm     "$ROOT/DEBIAN/"
install -m 755 deploy/debian/postrm    "$ROOT/DEBIAN/"

INSTALLED_SIZE="$(du -sk --exclude=DEBIAN "$ROOT" | cut -f1)"
sed -e "s/@VERSION@/${DEB_VERSION}/" \
    -e "s/@ARCH@/${ARCH}/" \
    -e "s/@INSTALLED_SIZE@/${INSTALLED_SIZE}/" \
    deploy/debian/control > "$ROOT/DEBIAN/control"
if grep -q '@[A-Z_]*@' "$ROOT/DEBIAN/control"; then
    echo "deploy/debian/control has an unsubstituted placeholder:" >&2
    grep '@[A-Z_]*@' "$ROOT/DEBIAN/control" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
dpkg-deb --build --root-owner-group -Zxz "$ROOT" "$OUT_DIR/${PACKAGE}.deb"
dpkg-deb --info "$OUT_DIR/${PACKAGE}.deb"
echo "Built $OUT_DIR/${PACKAGE}.deb"
