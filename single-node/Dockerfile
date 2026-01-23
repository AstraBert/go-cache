FROM golang:1.24-bookworm AS builder
WORKDIR /build
COPY go.mod ./
RUN go mod download
COPY *.go ./
RUN go build -tags netgo -ldflags '-s -w' -o server

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && \
    apt-get install -y ca-certificates sqlite3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/server /app/server

EXPOSE 8000
CMD ["./server"]
