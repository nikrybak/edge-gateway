# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/edge-gateway ./cmd/edge-gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/edge-gateway /usr/local/bin/edge-gateway
COPY configs/docker.yaml /etc/edge-gateway/config.yaml
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/edge-gateway"]
CMD ["-config", "/etc/edge-gateway/config.yaml"]
