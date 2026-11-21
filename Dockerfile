FROM rust:1.92.0-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2 AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo/merged.profdata /pgo/merged.profdata

ENV CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    RUSTFLAGS="-C target-cpu=cascadelake -C profile-use=/pgo/merged.profdata -C llvm-args=-pgo-warn-missing-function"
RUN rustc -Vv && cargo build --locked --release

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
ENV RAYON_NUM_THREADS=8
COPY --from=build /src/target/release/shopping-native /app/shopping-native
RUN mkdir -p /output /scratch && chown -R 1000:1000 /output /scratch
USER 1000:1000
CMD ["/app/shopping-native"]
