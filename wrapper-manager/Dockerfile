FROM golang:1.25 AS builder

ARG WRAPPER_MANAGER_REF=4a8859d066af8d9da99ef25006a449b3f161bbd8
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && git init \
    && git remote add origin https://github.com/WorldObservationLog/wrapper-manager.git \
    && git fetch --depth 1 origin "${WRAPPER_MANAGER_REF}" \
    && git checkout --detach FETCH_HEAD \
    && rm -rf /var/lib/apt/lists/*
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o wrapper-manager

FROM ubuntu:24.04
WORKDIR /root/
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/wrapper-manager ./wrapper-manager
RUN chmod +x ./wrapper-manager
VOLUME ["/root/data"]
ENTRYPOINT ["./wrapper-manager"]
EXPOSE 8080
