FROM golang:1.22 AS builder
ENV CGO_ENABLED=0

WORKDIR /src
COPY . /src

RUN go mod tidy
WORKDIR /src/app/events-api
RUN go build

# We don't need golang to run binaries, just use alpine.
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /src/app/events-api/events-api /app/events-api
RUN chmod +x /app/events-api

EXPOSE 8000
EXPOSE 8001

WORKDIR /app

ENTRYPOINT ["./events-api"]
