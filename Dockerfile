FROM golang:1.24 AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/simple-volume ./cmd/simple-volume

FROM debian:13-slim

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

