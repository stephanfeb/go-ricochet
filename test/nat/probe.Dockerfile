# The NAT probe. Build context: the repository root (see compose.yml).

FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/probe ./test/nat/probe

FROM debian:bookworm-slim
# iproute2: the probe routes the simulated internet through its NAT router.
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/probe /usr/local/bin/probe
COPY test/nat/probe-entrypoint.sh /usr/local/bin/probe-entrypoint.sh
RUN chmod +x /usr/local/bin/probe-entrypoint.sh
ENTRYPOINT ["probe-entrypoint.sh"]
