# The Ricochet server, built from this repository's source.
# Build context: the repository root (see compose.yml).

FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ricochet ./cmd/ricochet

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ricochet /usr/local/bin/ricochet
COPY examples/mailbox/docker/server-entrypoint.sh /usr/local/bin/server-entrypoint.sh
RUN chmod +x /usr/local/bin/server-entrypoint.sh
EXPOSE 55223/udp
ENTRYPOINT ["server-entrypoint.sh"]
