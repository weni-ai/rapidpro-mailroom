FROM golang:1.21-alpine

WORKDIR /app

RUN apk add --no-cache build-base git curl tar

RUN addgroup -S golang \
  && adduser -S -G golang golang

COPY . .

RUN curl -L https://github.com/nyaruka/goflow/releases/download/v$(grep goflow go.mod | cut -d" " -f2 | cut -c2-)/docs.tar.gz --output docs.tar.gz && \
    tar -xf docs.tar.gz && rm docs.tar.gz


RUN go install -v ./cmd/...

EXPOSE 8000
ENTRYPOINT ["mailroom"]
