ARG GOVERSION=1.25.5

# Temari's Go binding uses purego/dlopen. That makes the otherwise CGO-free Go
# executable dynamically depend on the builder's libc loader. Build on glibc so
# the executable is compatible with the Ubuntu runtime below; an Alpine (musl)
# build fails at container start with the misleading "no such file or directory".
FROM --platform=$BUILDPLATFORM golang:${GOVERSION}-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod tidy && \
    TEMARI_DIR="$(go list -m -f '{{.Dir}}' github.com/WorldObservationLog/Temari/bindings/go)" && \
    case "${TARGETARCH}" in amd64) TEMARI_ARCH=x86_64 ;; arm64) TEMARI_ARCH=arm64 ;; *) echo "Unsupported Temari architecture: ${TARGETARCH}" >&2; exit 1 ;; esac && \
    cp "${TEMARI_DIR}/lib/linux-${TEMARI_ARCH}/libtemari.so" /bin/libtemari.so && \
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
COPY --from=builder /bin/libtemari.so /usr/local/lib/libtemari.so
WORKDIR /app
# Compose bind-mounts the generated config.yaml in production. Keep a harmless
# fallback in the image for standalone inspection/CI without making generated
# config part of the build context (which would invalidate the Go build cache).
COPY config.yaml.example ./config.yaml
RUN echo 'alac-save-folder: "/downloads/ALAC"' >> config.yaml \
    && echo 'atmos-save-folder: "/downloads/Atmos"' >> config.yaml \
    && echo 'aac-save-folder: "/downloads/AAC"' >> config.yaml
ENTRYPOINT ["/usr/local/bin/apple-music-dl"]
