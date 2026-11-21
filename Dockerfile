FROM rust:1.92-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2 AS writer-build
WORKDIR /writer
COPY writer/Cargo.toml writer/Cargo.lock ./
COPY writer/src ./src
RUN cargo build --locked --release --lib

FROM rust:1.92-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2 AS fallback-build
WORKDIR /fallback
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo/merged.profdata /pgo/merged.profdata
ENV CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    RUSTFLAGS="-C target-cpu=cascadelake -C profile-use=/pgo/merged.profdata -C llvm-args=-pgo-warn-missing-function"
RUN cargo build --locked --release

FROM gcc:14-bookworm@sha256:5e927c284bf55a7dc796262e311a0703344f62f41f5621eb56843111b1d37e15 AS build
WORKDIR /src
COPY fastengine/engine.cpp fastengine/identity.hpp fastengine/fastengine_parquet.h ./
COPY --from=writer-build /writer/target/release/libfastengine_parquet.a /lib/libfastengine_parquet.a
RUN g++ -std=c++20 -O3 -DNDEBUG -march=cascadelake -mtune=cascadelake -pthread \
    -static-libstdc++ -static-libgcc engine.cpp /lib/libfastengine_parquet.a -ldl -lm -o engine

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /src/engine /engine
COPY --from=fallback-build /fallback/target/release/shopping-native /fallback
USER 1000:1000
ENTRYPOINT ["/engine"]
