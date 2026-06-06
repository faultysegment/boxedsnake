FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=linux go build -o history ./cmd/history

FROM alpine:latest

WORKDIR /app
RUN apk --no-cache add ca-certificates

COPY --from=builder /app/history .

EXPOSE 8082

ENTRYPOINT ["./history"]
