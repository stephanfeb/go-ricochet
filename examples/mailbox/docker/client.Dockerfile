# The mailbox example client.
# Build context: the repository root (see compose.yml).

FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/mailbox ./examples/mailbox

FROM debian:bookworm-slim
# iproute2: the client routes the simulated internet through its NAT router.
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/mailbox /usr/local/bin/mailbox
COPY examples/mailbox/docker/client-entrypoint.sh /usr/local/bin/client-entrypoint.sh
RUN chmod +x /usr/local/bin/client-entrypoint.sh
ENTRYPOINT ["client-entrypoint.sh"]
