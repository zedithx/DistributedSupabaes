FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/supaswarm ./cmd/supaswarm

FROM alpine:3.21

WORKDIR /app
COPY --from=builder /out/supaswarm /app/supaswarm

EXPOSE 8080

ENTRYPOINT ["/app/supaswarm"]
