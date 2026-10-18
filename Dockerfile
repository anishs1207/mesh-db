# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go test ./... && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mesh-db ./cmd/mesh-db

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mesh-db /mesh-db
VOLUME ["/data"]
EXPOSE 7000 9090
ENTRYPOINT ["/mesh-db"]
CMD ["-port", "7000", "-data-dir", "/data"]
