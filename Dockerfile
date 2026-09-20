FROM golang:1.27.0-bookworm@sha256:ded31c68586d2e49e760acc2e65a884b23d032e9bbbed0ae0c55abd3fcaf4452 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOMAXPROCS=2 go build -p 2 -trimpath -ldflags="-X main.version=${VERSION}" -o /out/row-relay ./cmd/row-relay

FROM scratch
ARG VERSION=dev
LABEL org.opencontainers.image.source="https://github.com/sleepkqq/row-relay" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/row-relay /row-relay
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY LICENSE /licenses/row-relay-LICENSE
COPY internal/pgque/LICENSE internal/pgque/NOTICE /licenses/pgque/
USER 65532:65532
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/row-relay"]
