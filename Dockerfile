FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/keelstone ./cmd/keelstone

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/keelstone /usr/local/bin/keelstone
EXPOSE 43121
ENTRYPOINT ["keelstone"]
