FROM alpine:3.20
RUN apk add --no-cache iproute2 iputils-ping ca-certificates wget
COPY keelstone keelstone-before bench /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/keelstone"]
