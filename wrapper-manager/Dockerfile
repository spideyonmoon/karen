FROM golang:1.23 AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y git ca-certificates && \
    git clone --depth 1 https://github.com/WorldObservationLog/wrapper-manager.git . && \
    sed -i 's/timeout     = 5 \* time.Second/timeout     = 25 \* time.Second/g' decrypt_instance.go

# Patch: safe nil checks in WebPlayback handler (upstream crashes on certain responses)
COPY webplay.go ./webplay.go

RUN go env -w GO111MODULE=on
RUN go mod tidy
RUN GOOS=linux go build -o wrapper-manager

FROM ubuntu:latest

WORKDIR /root/

COPY --from=builder /app/wrapper-manager .
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN chmod +x ./wrapper-manager

ENTRYPOINT ["./wrapper-manager"]
EXPOSE 8080
