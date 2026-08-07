# --- build stage ---
FROM golang:1.23-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/node ./cmd/node

# --- runtime stage ---
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/node ./node

# Default ports; overridden per-service in docker-compose.yml only for
# the published HTTP port - RAFT_PORT/HTTP_PORT env vars still control
# what the process itself actually binds to.
EXPOSE 8080 9090

ENTRYPOINT ["./node"]
