ARG GOVERSION=1.25.5

FROM --platform=$BUILDPLATFORM golang:${GOVERSION}-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN apk add --no-cache git
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /bin/apple-music-dl .

# Build mp4decrypt (Bento4) from source so it works on whatever arch the image targets.
# Music videos are Widevine-encrypted and decrypted with mp4decrypt (runv3 path);
# audio still goes through wrapper-manager and does not need this.
# Built on the same ubuntu base as the runtime stage for ABI compatibility.
FROM gpac/ubuntu AS bento4
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends git cmake g++ make ca-certificates && \
    git clone --depth 1 https://github.com/axiomatic-systems/Bento4 /bento4 && \
    cd /bento4 && mkdir cmakebuild && cd cmakebuild && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j"$(nproc)" mp4decrypt

FROM gpac/ubuntu
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*
COPY --from=bento4 /bento4/cmakebuild/mp4decrypt /usr/local/bin/mp4decrypt
COPY --from=builder /bin/apple-music-dl /usr/local/bin/apple-music-dl
WORKDIR /app
COPY config.yaml ./
RUN echo 'alac-save-folder: "/downloads/ALAC"' >> config.yaml \
    && echo 'atmos-save-folder: "/downloads/Atmos"' >> config.yaml \
    && echo 'aac-save-folder: "/downloads/AAC"' >> config.yaml
ENTRYPOINT ["/usr/local/bin/apple-music-dl"]
