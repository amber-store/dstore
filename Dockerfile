# The dstore node image: the dstore binary plus an environment-driven
# entrypoint (docker/entrypoint.sh) that creates a cluster, joins one or
# just serves, depending on DSTORE_ROLE and the state of the store.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /dstore ./cmd/dstore

FROM alpine:3.20
LABEL org.opencontainers.image.source=https://github.com/amber-store/dstore
LABEL org.opencontainers.image.description="dstore: a distributed amber store over iroh"
RUN apk add --no-cache ca-certificates
COPY --from=build /dstore /usr/local/bin/dstore
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
VOLUME ["/data"]
EXPOSE 4433/udp
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
