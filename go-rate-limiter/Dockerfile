# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy source files
COPY . .

# Compile optimized static binary
RUN CGO_ENABLED=0 GOOS=linux go build -o rate-limiter-app main.go

# Final stage
FROM alpine:latest

WORKDIR /root/

# Copy compiled binary from build stage
COPY --from=builder /app/rate-limiter-app .

# Expose server port
EXPOSE 8080

# Run binary
CMD ["./rate-limiter-app"]
