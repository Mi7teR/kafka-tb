# The TigerBeetle client is cgo: it links a prebuilt static library shipped in
# the Go module (native/libtb_client_*.a). That rules out CGO_ENABLED=0 and it
# rules out a scratch image — the binary still needs libc for -ldl and -lm.
# Deliberately NOT --platform=$BUILDPLATFORM. Pinning the builder to the build
# platform would need GOARCH cross-compilation, and cgo cannot cross-compile
# without a cross toolchain — the result is an amd64 binary inside an arm64
# image, which fails at exec with "Dynamic loader not found". Letting buildx run
# this stage under emulation per target architecture is slower and correct.
FROM golang:1.25-bookworm AS build

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# cache. The TigerBeetle module carries ~3 MB of static libraries.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# No GOOS/GOARCH here on purpose: this stage already runs on the target
# architecture, so the native toolchain produces the right binary and cgo links
# the matching TigerBeetle static library.
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
