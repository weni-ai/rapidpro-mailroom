FROM golang:1.26-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download -x

COPY . ./

RUN curl -L https://github.com/nyaruka/goflow/releases/download/v$(grep goflow go.mod | cut -d" " -f2 | cut -c2-)/docs.tar.gz --output docs.tar.gz && \
    tar -xf docs.tar.gz && rm docs.tar.gz

RUN GOOS=linux GOARCH=amd64 go build -o /bin/mailroom github.com/nyaruka/mailroom/v26/cmd/mailroom

FROM debian:bookworm-slim

# ca-certificates for outbound TLS; distroless had no shell, debian-slim provides /bin/sh
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /bin/mailroom ./
COPY --from=builder /src/docs ./docs

EXPOSE 8090
ENTRYPOINT ["./mailroom"]
