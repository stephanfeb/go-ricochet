#!/bin/bash
# Build Debian package for Ricochet Server (Go)
# This script runs on the host (macOS) and uses Docker for the build

set -e

VERSION="${VERSION:-1.0.0}"

echo "=========================================="
echo "Ricochet Server (Go) — Debian Package Build"
echo "Version: ${VERSION}"
echo "=========================================="
echo ""

# Step 1: Build the Docker image
echo "[1/3] Building Docker build environment..."
docker compose -f docker-compose.build.yml build
echo "✓ Build environment ready"
echo ""

# Step 2: Run the build inside the container
echo "[2/3] Building package inside container..."
docker compose -f docker-compose.build.yml run --rm \
    -e VERSION="${VERSION}" \
    -e HOST_USER_ID="$(id -u)" \
    -e HOST_GROUP_ID="$(id -g)" \
    builder \
    /bin/bash docker-build.sh
echo "✓ Container build complete"
echo ""

# Step 3: Verify output
echo "[3/3] Verifying package..."
if [ -f "build/dist/ricochet-server_${VERSION}.deb" ]; then
    echo "✓ Package created: build/dist/ricochet-server_${VERSION}.deb"
    echo ""
    ls -lh "build/dist/ricochet-server_${VERSION}.deb"
    echo ""
    echo "Installation (on Ubuntu):"
    echo "  sudo dpkg -i build/dist/ricochet-server_${VERSION}.deb"
    echo ""
    echo "Configuration:"
    echo "  1. Create /etc/ricochet/env with DB_PASSWORD=your_password"
    echo "  2. Initialize database: psql -U ricochet -d ricochet -f /opt/ricochet/schema.sql"
    echo "  3. Start: sudo supervisorctl start ricochet"
else
    echo "ERROR: Package not found at build/dist/ricochet-server_${VERSION}.deb"
    exit 1
fi
