FROM golang:1.26.10 AS build

# eMudhra CA compromise (2026-10-20): sanitize OS trust, including build tools.
RUN set -eu; \
    if command -v apk >/dev/null 2>&1; then \
      apk add --no-cache ca-certificates; \
    elif command -v apt-get >/dev/null 2>&1; then \
      apt-get update; apt-get install -y --no-install-recommends ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    else echo 'Unsupported CA store: review before building' >&2; exit 1; fi; \
    sed -i -E 's@^(mozilla/(Comodo_AAA_Services_root|AAA_Certificate_Services|emSign_ECC_Root_CA_-_G3|emSign_Root_TLS_CA_-_G3|emSign_Root_CS_CA_-_G3)\.crt)$@!\1@' /etc/ca-certificates.conf; \
    update-ca-certificates
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    NODE_OPTIONS=--use-openssl-ca

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/simple-volume ./cmd/simple-volume

FROM debian:13-slim

# eMudhra CA compromise (2026-10-20): sanitize OS trust, including build tools.
RUN set -eu; \
    if command -v apk >/dev/null 2>&1; then \
      apk add --no-cache ca-certificates; \
    elif command -v apt-get >/dev/null 2>&1; then \
      apt-get update; apt-get install -y --no-install-recommends ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    else echo 'Unsupported CA store: review before building' >&2; exit 1; fi; \
    sed -i -E 's@^(mozilla/(Comodo_AAA_Services_root|AAA_Certificate_Services|emSign_ECC_Root_CA_-_G3|emSign_Root_TLS_CA_-_G3|emSign_Root_CS_CA_-_G3)\.crt)$@!\1@' /etc/ca-certificates.conf; \
    update-ca-certificates
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    NODE_OPTIONS=--use-openssl-ca

ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
        mount \
        rclone \
        rsync \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/simple-volume /usr/local/bin/simple-volume

ENTRYPOINT ["/usr/local/bin/simple-volume"]

