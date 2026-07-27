FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/trader-controller ./cmd/controller

FROM alpine:3.20

RUN apk add --no-cache aws-cli ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

COPY --from=build /out/trader-controller /trader-controller
EXPOSE 8090
USER app:app
ENTRYPOINT ["/trader-controller"]
