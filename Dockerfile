FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o live .

FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /build/live /app/live

RUN addgroup -S app && adduser -S app -G app
USER app

CMD ["/app/live"]
