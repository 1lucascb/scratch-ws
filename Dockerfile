# ==========================================
# Build Stage
# ==========================================
FROM golang:1.27-alpine AS builder

# Set working directory
WORKDIR /app

# Cache dependencies before copying source code
COPY . .
RUN go mod download

# Compile static binary:
# - CGO_ENABLED=0 disables C library dependencies
# - -trimpath removes absolute build paths from traces
# - -ldflags="-s -w" strips debug symbols and DWARF tables
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /app/server .

# ==========================================
# Production Stage
# ==========================================
FROM gcr.io/distroless/static-debian13:nonroot

WORKDIR /

# Copy statically built binary from builder stage
COPY --from=builder /app/server /server

# Distroless static images provide 'nonroot' user (UID 65532) for security
USER nonroot:nonroot

# Expose target application port
EXPOSE 8000

ENTRYPOINT ["/server"]