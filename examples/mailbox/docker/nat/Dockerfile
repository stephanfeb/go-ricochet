# A home router: forwards and masquerades a private network onto the
# simulated internet.
FROM alpine:3.20
RUN apk add --no-cache iptables iproute2
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
