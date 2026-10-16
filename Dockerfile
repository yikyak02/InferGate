FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/simulator ./cmd/simulator && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/experiment ./cmd/experiment
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/ /
COPY configs /configs
USER 65532:65532
EXPOSE 8080 9001
ENTRYPOINT ["/gateway"]
CMD ["-config", "/configs/compose.json"]
