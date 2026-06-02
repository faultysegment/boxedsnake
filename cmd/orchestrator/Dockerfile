FROM golang:1.25 AS builder
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o orchestrator ./cmd/orchestrator

FROM alpine:latest
WORKDIR /app
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/orchestrator .

EXPOSE 8080
ENTRYPOINT ["./orchestrator"]
