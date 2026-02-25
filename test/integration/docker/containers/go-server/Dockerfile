# Ricochet server for store integration tests
# Uses the pre-built .deb package.
# Build context must be the agentic/ root directory.

FROM debian:bookworm-slim

# Install system dependencies (postgresql-client for pg_isready)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install the pre-built ricochet-server .deb (amd64)
COPY go-ricochet/build/dist/ricochet-server_1.0.0.deb /tmp/
RUN dpkg -i /tmp/ricochet-server_1.0.0.deb || true && \
    apt-get update && apt-get install -f -y && \
    rm /tmp/ricochet-server_1.0.0.deb && \
    test -x /opt/ricochet/ricochet_server

# Create required directories
RUN mkdir -p /var/lib/ricochet/sf_storage /var/log/ricochet /shared

# Copy entrypoint
COPY go-ricochet/test/integration/docker/containers/go-server/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 55223/udp

ENTRYPOINT ["/entrypoint.sh"]
