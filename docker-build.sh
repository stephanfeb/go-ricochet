#!/bin/bash
# Build script for use inside Docker container
# This script is executed inside the Ubuntu build container

set -e

VERSION="${VERSION:-1.0.0}"
PACKAGE_NAME="ricochet-server"
BUILD_DIR="build/package/${PACKAGE_NAME}_${VERSION}"

echo "=========================================="
echo "Building Ricochet Server v${VERSION}"
echo "Environment: Docker (Ubuntu 22.04)"
echo "=========================================="
echo ""

# Set up environment
export HOME=/home/builder
export GOPATH=/home/builder/go
export GOCACHE=/home/builder/.cache/go-build

# Ensure home directories and caches are writable
sudo mkdir -p $GOPATH/pkg/mod/cache $GOCACHE
sudo chown -R builder:builder $HOME $GOPATH $GOCACHE

# Check Go installation
echo "Go version:"
go version
echo ""

# Step 1: Copy source files to writable workspace
echo "[1/7] Copying source files..."
WORK_DIR=/home/builder/workspace/agentic
mkdir -p $WORK_DIR
rsync -a --exclude='build' --exclude='.idea' --exclude='*.iml' \
      --exclude='*.log' --exclude='test_storage' --exclude='sf_storage' \
      --exclude='*.deb' \
      /workspace/IdeaProjects/agentic/ $WORK_DIR/
cd $WORK_DIR/go-ricochet
echo "✓ Source files copied"
echo ""

# Step 2: Fix module replace directives to use workspace paths
echo "[2/7] Fixing module paths..."
sed -i \
    -e 's|=> \.\./go-libp2p-udx-transport|=> ../go-libp2p-udx-transport|g' \
    -e 's|=> \.\./go-udx|=> ../go-udx|g' \
    go.mod
echo "✓ Module paths configured"
echo ""

# Step 3: Get dependencies
echo "[3/7] Getting Go dependencies..."
go mod download
echo "✓ Dependencies resolved"
echo ""

# Step 4: Build the binary
echo "[4/7] Compiling Ricochet server for linux/amd64..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ricochet_server ./cmd/ricochet/
echo "✓ Binary compiled"
echo ""

# Step 5: Create package structure
echo "[5/7] Creating package structure..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/DEBIAN"
mkdir -p "$BUILD_DIR/opt/ricochet"
mkdir -p "$BUILD_DIR/etc/ricochet"
mkdir -p "$BUILD_DIR/etc/supervisor/conf.d"
mkdir -p "$BUILD_DIR/var/lib/ricochet"
mkdir -p "$BUILD_DIR/var/log/ricochet"
echo "✓ Package directories created"
echo ""

# Step 6: Copy files
echo "[6/7] Copying files..."
cp ricochet_server "$BUILD_DIR/opt/ricochet/"
cp schema.sql "$BUILD_DIR/opt/ricochet/"
cp deploy/run.sh "$BUILD_DIR/opt/ricochet/"
# Copy config as both .example and actual config (for conffiles)
cp config.example.yaml "$BUILD_DIR/etc/ricochet/config.yaml.example"
cp config.example.yaml "$BUILD_DIR/etc/ricochet/config.yaml"
cp deploy/env.example "$BUILD_DIR/etc/ricochet/"
cp deploy/supervisor/ricochet.conf "$BUILD_DIR/etc/supervisor/conf.d/"

# Copy control files
cp deploy/debian/control "$BUILD_DIR/DEBIAN/"
cp deploy/debian/conffiles "$BUILD_DIR/DEBIAN/"
cp deploy/debian/postinst "$BUILD_DIR/DEBIAN/"
cp deploy/debian/prerm "$BUILD_DIR/DEBIAN/"
cp deploy/debian/postrm "$BUILD_DIR/DEBIAN/"

# Set executable permissions
chmod 755 "$BUILD_DIR/DEBIAN/postinst"
chmod 755 "$BUILD_DIR/DEBIAN/prerm"
chmod 755 "$BUILD_DIR/DEBIAN/postrm"
chmod 755 "$BUILD_DIR/opt/ricochet/ricochet_server"
chmod 755 "$BUILD_DIR/opt/ricochet/run.sh"
echo "✓ Application files copied"
echo ""

# Step 7: Build .deb package
echo "[7/7] Building .deb package..."
dpkg-deb --build --root-owner-group "$BUILD_DIR"
echo "✓ Package built successfully"
echo ""

# Copy output to mounted build directory
mkdir -p /workspace/IdeaProjects/agentic/go-ricochet/build/dist
cp "build/package/${PACKAGE_NAME}_${VERSION}.deb" /workspace/IdeaProjects/agentic/go-ricochet/build/dist/

# Fix ownership of build output to match host user
if [ -n "$HOST_USER_ID" ] && [ -n "$HOST_GROUP_ID" ]; then
    echo "Fixing file permissions for host user..."
    chown -R $HOST_USER_ID:$HOST_GROUP_ID /workspace/IdeaProjects/agentic/go-ricochet/build 2>/dev/null || \
        sudo chown -R $HOST_USER_ID:$HOST_GROUP_ID /workspace/IdeaProjects/agentic/go-ricochet/build
fi

echo "=========================================="
echo "✓ Build complete!"
echo "=========================================="
echo ""
echo "Package created: build/dist/${PACKAGE_NAME}_${VERSION}.deb"
echo ""
echo "File info:"
ls -lh "/workspace/IdeaProjects/agentic/go-ricochet/build/dist/${PACKAGE_NAME}_${VERSION}.deb"
echo ""
echo "Installation (on Ubuntu):"
echo "  sudo dpkg -i build/dist/${PACKAGE_NAME}_${VERSION}.deb"
echo ""
