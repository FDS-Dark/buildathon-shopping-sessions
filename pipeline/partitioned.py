#!/usr/bin/env python3
"""Bounded-memory, end-to-end Shopping Sessions candidate."""

from __future__ import annotations

import glob
import json
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("POLARS_MAX_THREADS", os.environ.get("NCPU", "8"))

import polars as pl


DATA = Path(os.environ.get("DATA_DIR", sys.argv[1] if len(sys.argv) > 1 else "/data"))
OUT = Path(os.environ.get("OUTPUT_DIR", sys.argv[2] if len(sys.argv) > 2 else "/output"))
SCRATCH = Path(os.environ.get("SCRATCH_DIR", OUT / ".scratch"))
EVENT_INPUT_BYTES = sum(path.stat().st_size for path in (DATA / "events").glob("*.csv"))
# Four buckets are materially faster on practice-sized inputs. Eight bound the
# projected final peak below 20 GB while retaining good sequential I/O.
DEFAULT_BUCKETS = 8 if EVENT_INPUT_BYTES > 2_000_000_000 else 4
BUCKETS = int(os.environ.get("PARTITION_BUCKETS", str(DEFAULT_BUCKETS)))
DAY_MS = 86_400_000
SEVEN_DAYS_MS = 604_800_000


def exact_csv(filename: str | Path, columns: list[str]) -> pl.DataFrame:
    fields = [f'(?:"(?P<c{i}q>[^\",]*)"|(?P<c{i}u>[^\",]*))' for i in range(len(columns))]
    pattern = "^" + ",".join(fields) + "$"
    raw = pl.read_csv(
        filename,
        schema={"__raw": pl.String},
        has_header=False,
        skip_rows=1,
        separator="\x1f",
        quote_char=None,
        eol_char="\n",
        truncate_ragged_lines=True,
        row_index_name="__row",
    )
    parsed = raw.with_columns(pl.col("__raw").str.extract_groups(pattern).alias("__parsed")).unnest("__parsed")
    valid = pl.col("c0q").is_not_null() | pl.col("c0u").is_not_null()
    frame = parsed.filter(valid).select(
        "__row", *[pl.coalesce(f"c{i}q", f"c{i}u").alias(column) for i, column in enumerate(columns)]
    )
    if os.path.getsize(filename) < 1_000_000:
        lines = Path(filename).read_bytes().split(b"\n")[1:]
        cr_rows = [index for index, line in enumerate(lines) if line.endswith(b"\r")]
        if cr_rows:
            last = columns[-1]
            frame = frame.with_columns(
                pl.when(pl.col("__row").is_in(cr_rows))
                .then(pl.col(last) + "\r")
                .otherwise(pl.col(last))
                .alias(last)
            )
    return frame.drop("__row")


def timestamp_ms(column: str) -> pl.Expr:
    value = pl.col(column)
    epoch = pl.when(value.str.contains(r"^[0-9]+$")).then(value.cast(pl.Int64, strict=False))
    f1 = pl.when(
        value.str.contains(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$")
    ).then(value.str.strptime(pl.Datetime("ms", "UTC"), "%Y-%m-%dT%H:%M:%SZ", strict=False, exact=True).cast(pl.Int64))
    f2 = pl.when(
        value.str.contains(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][+-](?:[01][0-9]|2[0-3]):[0-5][0-9]$"
        )
    ).then(value.str.strptime(pl.Datetime("ms", "UTC"), "%Y-%m-%dT%H:%M:%S%:z", strict=False, exact=True).cast(pl.Int64))
    return pl.coalesce(epoch, f1, f2)


def f1_ms(column: str) -> pl.Expr:
    return pl.col(column).str.strptime(
        pl.Datetime("ms", "UTC"), "%Y-%m-%dT%H:%M:%SZ", strict=False, exact=True
    ).cast(pl.Int64)


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, value: str) -> str:
        parent = self.parent.setdefault(value, value)
        while parent != self.parent[parent]:
            self.parent[parent] = self.parent[self.parent[parent]]
            parent = self.parent[parent]
        while value != parent:
            nxt = self.parent[value]
            self.parent[value] = parent
            value = nxt
        return parent

    def union(self, left: str, right: str) -> None:
        a, b = self.find(left), self.find(right)
        if a != b:
            if b < a:
                a, b = b, a
            self.parent[b] = a


def write_buckets(frame: pl.DataFrame, column: str, target: Path, stem: str) -> int:
    frame = frame.with_columns((pl.col(column).hash(seed=0) % BUCKETS).cast(pl.UInt16).alias("__bucket"))
    written = 0
    for bucket, part in frame.partition_by("__bucket", as_dict=True).items():
        bucket_number = int(bucket[0] if isinstance(bucket, tuple) else bucket)
        folder = target / f"{bucket_number:03d}"
        folder.mkdir(parents=True, exist_ok=True)
        part.drop("__bucket").write_parquet(folder / f"{stem}.parquet", compression="lz4")
        written += part.height
    return written


def mark(label: str, started: float) -> float:
    now = time.perf_counter()
    print(f"{label}: {now - started:.3f}s", flush=True)
    return now


def main() -> None:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    eid_root = SCRATCH / "eid"
    cid_root = SCRATCH / "cid"
    event_person_root = SCRATCH / "event-person"
    click_person_root = SCRATCH / "click-person"
    partial_user_root = SCRATCH / "partial-user"
    partial_credit_root = SCRATCH / "partial-credit"
    partial_assist_root = SCRATCH / "partial-assist"
    for folder in (partial_user_root, partial_credit_root, partial_assist_root):
        folder.mkdir(parents=True, exist_ok=True)

    with (DATA / "manifest.json").open() as file:
        manifest = json.load(file)
    window_start = int(pl.Series([manifest["window_start"]]).str.strptime(pl.Datetime("ms", "UTC")).cast(pl.Int64)[0])
    window_end = int(pl.Series([manifest["window_end"]]).str.strptime(pl.Datetime("ms", "UTC")).cast(pl.Int64)[0])
    products = pl.read_csv(DATA / "products.csv", schema={"product_id": pl.String, "category": pl.String})
    campaigns = pl.read_csv(DATA / "campaigns.csv", schema={"campaign_id": pl.String, "channel": pl.String})

    links = []
    for path in sorted(glob.glob(str(DATA / "links" / "*.csv"))):
        links.append(exact_csv(path, ["user_id_a", "user_id_b"]))
    uf = UnionFind()
    for left, right in pl.concat(links).iter_rows():
        if left not in (None, "") and right not in (None, ""):
            uf.union(left, right)
    identity = pl.DataFrame({"raw_user_id": list(uf.parent), "person": [uf.find(x) for x in uf.parent]})

    def attach_person(frame: pl.DataFrame) -> pl.DataFrame:
        if identity.is_empty():
            return frame.with_columns(pl.col("user_id").alias("person"))
        return (
            frame.join(identity, left_on="user_id", right_on="raw_user_id", how="left")
            .with_columns(pl.coalesce("person", "user_id").alias("person"))
        )

    started = time.perf_counter()
    event_columns = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
    source_paths = sorted(glob.glob(str(DATA / "events" / "*.csv")))
    rows = 0
    pending: list[pl.DataFrame] = []
    pending_rows = 0
    batch_index = 0
    for source_index, path in enumerate(source_paths):
        events = exact_csv(path, event_columns).filter(pl.col("event_id").is_not_null() & (pl.col("event_id") != ""))
        events = events.with_columns(timestamp_ms("ts").alias("ts_ms"), f1_ms("ingest_ts").alias("ingest_ms")).select(
            "event_id", "user_id", "ts_ms", "ingest_ms", "event_type", "product_id", "price_cents"
        )
        pending.append(events)
        pending_rows += events.height
        if pending_rows >= 500_000 or source_index + 1 == len(source_paths):
            batch = pl.concat(pending, how="vertical_relaxed", rechunk=True)
            rows += write_buckets(batch, "event_id", eid_root, f"batch-{batch_index:04d}")
            pending.clear()
            pending_rows = 0
            batch_index += 1
    after_input = mark(f"stage1a parse+event partition ({rows} rows)", started)

    click_columns = ["click_id", "user_id", "campaign_id", "ts"]
    click_source_paths = sorted(glob.glob(str(DATA / "clicks" / "*.csv")))
    click_rows = 0
    pending = []
    pending_rows = 0
    batch_index = 0
    for source_index, path in enumerate(click_source_paths):
        clicks = exact_csv(path, click_columns).filter(pl.col("click_id").is_not_null() & (pl.col("click_id") != ""))
        clicks = clicks.with_columns(timestamp_ms("ts").alias("click_ms")).select(
            "click_id", "user_id", "campaign_id", "click_ms"
        )
        pending.append(clicks)
        pending_rows += clicks.height
        if pending_rows >= 500_000 or source_index + 1 == len(click_source_paths):
            batch = pl.concat(pending, how="vertical_relaxed", rechunk=True)
            click_rows += write_buckets(batch, "click_id", cid_root, f"batch-{batch_index:04d}")
            pending.clear()
            pending_rows = 0
            batch_index += 1
    after_click_input = mark(f"stage1b parse+click partition ({click_rows} rows)", after_input)

    survivors = 0
    for bucket in range(BUCKETS):
        paths = sorted((eid_root / f"{bucket:03d}").glob("*.parquet"))
        if not paths:
            continue
        events = pl.read_parquet(paths)
        events = (
            events.sort(["event_id", "ingest_ms"], nulls_last=True)
            .unique("event_id", keep="first", maintain_order=True)
            .with_columns(
                pl.col("price_cents").str.contains(r"^[0-9]+$").alias("price_digits"),
                pl.col("price_cents").cast(pl.Int64, strict=False).alias("price"),
            )
            .filter(
                pl.col("user_id").is_not_null()
                & (pl.col("user_id") != "")
                & pl.col("ts_ms").is_not_null()
                & (pl.col("ts_ms") >= window_start)
                & (pl.col("ts_ms") < window_end)
                & pl.col("event_type").is_in(["view", "cart", "purchase"])
                & pl.col("product_id").is_not_null()
                & (pl.col("product_id") != "")
                & (
                    (pl.col("event_type") != "purchase")
                    | (pl.col("price_digits") & pl.col("price").is_between(1, 10_000_000, closed="both"))
                )
                & ((pl.col("ingest_ms") - pl.col("ts_ms")) <= DAY_MS)
            )
        )
        events = attach_person(events).select("person", "ts_ms", "event_type", "product_id", "price")
        survivors += write_buckets(events, "person", event_person_root, f"eid-{bucket:03d}")
    after_event_dedup = mark(f"stage2a event dedup+validate+person partition ({survivors} rows)", after_click_input)

    valid_clicks = 0
    for bucket in range(BUCKETS):
        paths = sorted((cid_root / f"{bucket:03d}").glob("*.parquet"))
        if not paths:
            continue
        clicks = (
            pl.read_parquet(paths)
            .unique("click_id", keep="first")
            .filter(
                pl.col("user_id").is_not_null()
                & (pl.col("user_id") != "")
                & pl.col("click_ms").is_not_null()
                & (pl.col("click_ms") >= window_start - SEVEN_DAYS_MS)
                & (pl.col("click_ms") < window_end)
            )
            .join(campaigns, on="campaign_id", how="inner")
        )
        clicks = attach_person(clicks).select("person", "click_ms", "click_id", "campaign_id", "channel")
        valid_clicks += write_buckets(clicks, "person", click_person_root, f"cid-{bucket:03d}")
    after_click_dedup = mark(
        f"stage2b click dedup+validate+person partition ({valid_clicks} rows)", after_event_dedup
    )

    session_count = 0
    max_bucket_rows = 0
    for bucket in range(BUCKETS):
        event_paths = sorted((event_person_root / f"{bucket:03d}").glob("*.parquet"))
        if not event_paths:
            continue
        events = pl.read_parquet(event_paths).sort(["person", "ts_ms"])
        max_bucket_rows = max(max_bucket_rows, events.height)
        events = events.with_columns(
            (
                (pl.col("person") != pl.col("person").shift(1))
                | ((pl.col("ts_ms") - pl.col("ts_ms").shift(1)) > 1_800_000)
            ).fill_null(True).cum_sum().alias("session_id")
        )
        sessions = (
            events.group_by(["session_id", "person"])
            .agg(
                pl.col("ts_ms").min().alias("session_start"),
                pl.len().alias("events"),
                pl.when(pl.col("event_type") == "purchase")
                .then(pl.col("price"))
                .otherwise(0)
                .sum()
                .alias("revenue_cents"),
                (pl.col("event_type") == "purchase").any().alias("has_purchase"),
            )
            .with_columns(pl.from_epoch("session_start", time_unit="ms").dt.date().alias("day"))
        )
        session_count += sessions.height

        user_daily = sessions.group_by(["person", "day"]).agg(
            pl.len().alias("sessions"), pl.col("events").sum(), pl.col("revenue_cents").sum()
        )
        category_daily = (
            events.filter(pl.col("event_type") == "purchase")
            .select("session_id", "person", "product_id", "price")
            .join(sessions.select("session_id", "day"), on="session_id", how="left")
            .join(products, on="product_id", how="left")
            .with_columns(pl.col("category").fill_null("unknown"))
            .group_by(["person", "day", "category"])
            .agg(pl.col("price").sum().alias("category_revenue"))
            .sort(
                ["person", "day", "category_revenue", "category"],
                descending=[False, False, True, False],
            )
            .unique(["person", "day"], keep="first", maintain_order=True)
            .select("person", "day", pl.col("category").alias("top_category"))
        )
        user_daily = (
            user_daily.join(category_daily, on=["person", "day"], how="left")
            .rename({"person": "user_id"})
            .select("user_id", "day", "sessions", "events", "revenue_cents", "top_category")
        )
        user_daily.write_parquet(partial_user_root / f"part-{bucket:03d}.parquet", compression="lz4")

        click_paths = sorted((click_person_root / f"{bucket:03d}").glob("*.parquet"))
        if click_paths:
            clicks = pl.read_parquet(click_paths)
            ordered_clicks = clicks.sort(
                ["person", "click_ms", "click_id"], descending=[False, False, True]
            )
            credited = sessions.sort(["person", "session_start"]).join_asof(
                ordered_clicks.select("person", "click_ms", "click_id", "campaign_id", "channel"),
                left_on="session_start",
                right_on="click_ms",
                by="person",
                strategy="backward",
                tolerance=SEVEN_DAYS_MS,
            )
        else:
            clicks = pl.DataFrame(
                schema={
                    "person": pl.String,
                    "click_ms": pl.Int64,
                    "click_id": pl.String,
                    "campaign_id": pl.String,
                    "channel": pl.String,
                }
            )
            credited = sessions.with_columns(
                pl.lit(None, dtype=pl.String).alias("campaign_id"),
                pl.lit(None, dtype=pl.String).alias("channel"),
            )
        credited = credited.with_columns(
            pl.col("campaign_id").fill_null("organic"), pl.col("channel").fill_null("organic")
        )
        credited_daily = credited.group_by(["campaign_id", "channel", "day"]).agg(
            pl.len().alias("sessions"),
            pl.col("has_purchase").sum().alias("purchasing_sessions"),
            pl.col("revenue_cents").sum(),
        )
        credited_daily.write_parquet(partial_credit_root / f"part-{bucket:03d}.parquet", compression="lz4")

        if not clicks.is_empty():
            coverage = (
                clicks.select("person", "campaign_id", "click_ms")
                .unique()
                .sort(["person", "campaign_id", "click_ms"])
                .with_columns(
                    (
                        (pl.col("person") != pl.col("person").shift(1))
                        | (pl.col("campaign_id") != pl.col("campaign_id").shift(1))
                        | ((pl.col("click_ms") - pl.col("click_ms").shift(1)) > SEVEN_DAYS_MS)
                    )
                    .fill_null(True)
                    .cum_sum()
                    .alias("coverage_id")
                )
                .group_by(["coverage_id", "person", "campaign_id"])
                .agg(
                    pl.col("click_ms").min().alias("coverage_start"),
                    (pl.col("click_ms").max() + SEVEN_DAYS_MS).alias("coverage_end"),
                )
            )
            assist_pairs = sessions.select("session_id", "person", "session_start", "day").join_where(
                coverage.select("person", "campaign_id", "coverage_start", "coverage_end"),
                pl.col("person") == pl.col("person_right"),
                pl.col("coverage_start") <= pl.col("session_start"),
                pl.col("coverage_end") >= pl.col("session_start"),
                suffix="_right",
            )
            assisted_daily = assist_pairs.group_by(["campaign_id", "day"]).agg(
                pl.len().alias("assisted_sessions")
            )
            assisted_daily.write_parquet(
                partial_assist_root / f"part-{bucket:03d}.parquet", compression="lz4"
            )

    after_people = mark(f"stage3 person processing ({session_count} sessions)", after_click_dedup)

    user_paths = sorted(partial_user_root.glob("*.parquet"))
    user_daily = pl.read_parquet(user_paths) if user_paths else pl.DataFrame(
        schema={
            "user_id": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "events": pl.Int64,
            "revenue_cents": pl.Int64,
            "top_category": pl.String,
        }
    )
    credit_paths = sorted(partial_credit_root.glob("*.parquet"))
    if credit_paths:
        credited_daily = pl.read_parquet(credit_paths).group_by(["campaign_id", "channel", "day"]).agg(
            pl.col("sessions").sum(),
            pl.col("purchasing_sessions").sum(),
            pl.col("revenue_cents").sum(),
        )
    else:
        credited_daily = pl.DataFrame(
            schema={
                "campaign_id": pl.String,
                "channel": pl.String,
                "day": pl.Date,
                "sessions": pl.Int64,
                "purchasing_sessions": pl.Int64,
                "revenue_cents": pl.Int64,
            }
        )
    assist_paths = sorted(partial_assist_root.glob("*.parquet"))
    if assist_paths:
        assisted_daily = pl.read_parquet(assist_paths).group_by(["campaign_id", "day"]).agg(
            pl.col("assisted_sessions").sum()
        )
    else:
        assisted_daily = pl.DataFrame(
            schema={"campaign_id": pl.String, "day": pl.Date, "assisted_sessions": pl.Int64}
        )
    campaign_daily = (
        credited_daily.join(assisted_daily, on=["campaign_id", "day"], how="full", coalesce=True)
        .join(campaigns, on="campaign_id", how="left", suffix="_lookup")
        .with_columns(
            pl.coalesce("channel", "channel_lookup", pl.lit("organic")).alias("channel"),
            pl.col("sessions").fill_null(0),
            pl.col("purchasing_sessions").fill_null(0),
            pl.col("revenue_cents").fill_null(0),
            pl.col("assisted_sessions").fill_null(0),
        )
        .select(
            "campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"
        )
    )

    schemas = {
        "user_daily": {
            "user_id": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "events": pl.Int64,
            "revenue_cents": pl.Int64,
            "top_category": pl.String,
        },
        "campaign_daily": {
            "campaign_id": pl.String,
            "channel": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "purchasing_sessions": pl.Int64,
            "revenue_cents": pl.Int64,
            "assisted_sessions": pl.Int64,
        },
    }
    for name, frame in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
        folder = OUT / name
        folder.mkdir(parents=True, exist_ok=True)
        frame.cast(schemas[name]).write_parquet(folder / "part-0.parquet", compression="zstd")

    finished = mark("stage4 reduce+write", after_people)
    scratch_bytes = sum(path.stat().st_size for path in SCRATCH.rglob("*.parquet"))
    print(f"total: {finished - started:.3f}s")
    print(f"max person bucket: {max_bucket_rows} rows ({max_bucket_rows / max(survivors, 1):.2%})")
    print(f"scratch parquet: {scratch_bytes / 1_000_000:.1f} MB")


if __name__ == "__main__":
    main()


