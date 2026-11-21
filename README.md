# Shopping Sessions

An exact, clean-room native pipeline for the Databricks Shopping Sessions track. The runtime is a C++20 engine with a Rust static-library Parquet writer.

The implementation passes the official edge-case corpus, exactly matches both public 1M tables, exactly matches validated 20M outputs, and reproduces validated 60M aggregate totals.

## Architecture

- Chunked `pread` scanner with exact challenge CSV semantics and specialized common timestamp paths.
- Lossless packed common IDs with byte-exact generic fallbacks.
- Actual link-graph union-find with independently tracked byte-wise person minima.
- Dense, owner-partitioned event deduplication preserving dedup-before-validation.
- Compact 12-byte events and guarded dense-ID acceleration.
- Half-hour occupancy sessionization with exact 30-minute boundaries.
- Person-range click routing and parallel exact attribution/assist sweeps.
- Direct C++ to Rust FFI for plain, uncompressed Parquet output.
- A separately built exact Rust engine handles datasets that fail the globally validated dense-ID fast-path guards.
- Explicit support for the scorer's read-only root, uid 1000, eight pinned vCPUs, and offline runtime.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .

docker run --rm --platform linux/amd64 \
  --cpus 8 --memory 24g --memory-swap 24g --pids-limit 4096 \
  --network none --read-only --tmpfs /tmp:rw,size=64m \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
  -v "$DATA:/data:ro" -v "$OUTPUT:/output" -v "$SCRATCH:/scratch" \
  shopping-sessions:latest
```

The image writes `/output/user_daily/*.parquet` and `/output/campaign_daily/*.parquet` and logs stage timings to stderr.

## Files

- `fastengine/engine.cpp`: parsing, identity resolution, deduplication, sessions, attribution, and aggregation.
- `fastengine/identity.hpp`: packed-ID and exact link-graph identity support.
- `writer/src/lib.rs`: low-level Parquet writer exposed through a static C ABI.
- `Dockerfile`: pinned `linux/amd64` multi-stage build.
- `rules.md`: authoritative track specification.

The older Rust/Python experiments remain in repository history but are not copied into or used by the submitted image.
