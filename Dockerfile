# The TigerBeetle client is cgo: it links a prebuilt static library shipped in
# the Go module (native/libtb_client_*.a). That rules out CGO_ENABLED=0 and it
# rules out a scratch image — the binary still needs libc for -ldl and -lm.
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# cache. The TigerBeetle module carries ~3 MB of static libraries.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# TARGETARCH comes from buildx. Cross-compiling cgo needs a cross toolchain, so
# this image is built per-architecture under emulation rather than cross-built;
# BUILDPLATFORM above keeps the toolchain native while the target stays honest.
ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags="-s -w" -o /out/kafkatb ./cmd/kafkatb

# distroless/base carries glibc and CA certificates and nothing else: no shell,
# no package manager. It runs as nonroot by default.
FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/kafkatb /usr/local/bin/kafkatb
# The example config is the default value of --config, so an image without it
# cannot start with no arguments at all.
COPY --from=build /src/configs/example.yaml /etc/kafkatb/config.yaml

# metrics, health and readiness
EXPOSE 9464

ENTRYPOINT ["/usr/local/bin/kafkatb"]
CMD ["run", "--config", "/etc/kafkatb/config.yaml"]
