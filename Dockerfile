FROM golang:1.11-alpine3.8
ENV CGO_ENABLED=0
WORKDIR /go/src/github.com/rbone/migration
RUN apk --no-cache --update add git && \
  wget -q -O /go/bin/dep https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64 && \
  chmod 755 /go/bin/dep

