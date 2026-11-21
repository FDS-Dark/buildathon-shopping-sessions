# Shopping Sessions

An exact, optimized native Rust pipeline for the Databricks Shopping Sessions track. It resolves linked identities, cleans and deduplicates events and clicks, builds sessions, attributes and assists campaigns, and writes both required Parquet tables directly.

The implementation passes all 36 official edge-case fixtures and exactly matches both official tables for the 1M-event public development set.

## Implementation

- An allocation-conscious mmap parser implements the exact challenge dialect.
- Identity links use iterative union-by-size with separately tracked byte-wise minima.
- Guarded generator-ID recovery enables dense direct deduplication with an exact generic fallback.
- Compact records, parallel person-aligned sweeps, and direct Parquet output minimize memory traffic.
- The release binary is trained with a pinned Linux PGO profile and targets the scorer's Cascade Lake CPU.
- The image honors the scorer's CPU, memory, filesystem, UID, and offline runtime constraints.

## Run and test locally

You need Docker with buildx. Python 3.10+ with `polars` is used only by the comparison scripts.

Build the image and run all 36 fixtures:

```sh
make build        # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures     # runs the image on all 36 fixtures with scorer flags, diffs each against expected/
```

Build, run, and check the 1M-event dev set. `make dev` needs `generate` on your PATH: build it from the track page's generator.zip (Rust 1.85 or newer; its README has the two commands).

```sh
make dev                                # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                            # runs your image on dev/ with scorer flags and times it
make check-dev                          # exact diff against dev-answer/, the track page's dev-answer.zip unzipped here
```

If your laptop cannot spare 8 cores and 24 GB, run with lower limits using `make run-dev CPUS=4 MEM=8g`. Your local times will not match the scorer anyway; only the board's times count.

- **x86 Linux and Windows machines.** Build and run the amd64 image natively.
- **Apple-silicon Macs.** The optimized image targets Cascade Lake and must be built as `linux/amd64`; emulation is expected to be slow.

To build the native binary without Docker on x86-64:

```sh
cargo build --release --locked
```

## Submitting and scoring

Submit your public GitHub repository on the Buildathon track page. Enter a commit SHA to score a specific revision, or leave it empty to score the newest commit on the default branch.

The image's `CMD` runs with no arguments. Exit code 0 means "done, check my output". Build time is never timed; only the run is timed. The track page lists the current practice limits and how the final is scored.

## Container limits

Scorers are Google Cloud n2 Intel Xeon machines, Cascade Lake or newer, with AVX-512 available. Your container gets:

| Resource | Setting |
| --- | --- |
| `/data` | The dataset, read-only |
| `/output` | Empty, writable. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet |
| `/scratch` | Empty, writable, fast local disk for spills and temp files |
| CPU / RAM | 8 vCPUs, hyperthreads of an Intel Xeon, 24 GB, no swap |
| Network | None |
| Env vars | `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `SCRATCH_DIR=/scratch`, `HOME=/scratch`, `NCPU=8`, `MEM_GB=24` |
| Filesystem | Root is read-only apart from `/tmp`, 64 MB in memory. The process runs as uid 1000, not root |

## Common pitfalls

- **CSV semantics.** The challenge dialect is narrower than general CSV; the native parser preserves literal carriage returns and rejects malformed quoting and widths exactly.
- **Fast-path safety.** Dense-ID acceleration is guarded and falls back to exact byte-key processing when generator invariants do not hold.
- **Threads.** Runtime parallelism is capped at eight Rayon workers to match the scorer.
- **Memory.** The full 60M rehearsal stays in memory and requires no scratch spill.
- **Extra files in output folders.** `_SUCCESS` and other non-Parquet files in the output folders are ignored.

## Self-checks

- Summed over all rows, `sessions` and `revenue_cents` are equal in both tables.
- `day` must be a Parquet `DATE`, not a string or timestamp.
- Row order and the number of Parquet files do not matter. Exact column names do, and there must be no extra columns.

## Files

- `src/main.rs` holds the native pipeline.
- `pgo/merged.profdata` is the pinned Linux training profile for this exact source revision.
- `Dockerfile` builds a `linux/amd64` image for the Intel Xeon scorers.
- `fixtures/` holds the 36 official edge cases and expected tables.
- `scripts/` runs your image the way the scorer does and diffs the output.
