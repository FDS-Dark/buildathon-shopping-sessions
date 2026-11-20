#!/usr/bin/env python3
"""Select the eager or bounded-memory Shopping Sessions pipeline."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


AUTO_PARTITION_BYTES = 2_500_000_000
PIPELINES = {
    "eager": "pipeline.py",
    "partitioned": "partitioned.py",
}


def regular_input_bytes(root: Path) -> int:
    total = 0
    pending = [root]
    while pending:
        folder = pending.pop()
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
    return total


def main() -> None:
    requested = os.environ.get("PIPELINE_MODE", "auto").lower()
    if requested not in {"auto", *PIPELINES}:
        choices = "auto, eager, partitioned"
        raise SystemExit(f"invalid PIPELINE_MODE={requested!r}; expected one of: {choices}")

    if requested == "auto":
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        input_bytes = regular_input_bytes(data_dir)
        selected = "partitioned" if input_bytes >= AUTO_PARTITION_BYTES else "eager"
        print(
            f"pipeline mode: auto -> {selected} ({input_bytes} regular input bytes)",
            file=sys.stderr,
        )
    else:
        selected = requested
        print(f"pipeline mode: {selected}", file=sys.stderr)

    script = Path(__file__).with_name(PIPELINES[selected])
    command = [sys.executable, str(script), *sys.argv[1:]]
    if os.name == "nt":
        raise SystemExit(subprocess.call(command))
    os.execv(sys.executable, command)


if __name__ == "__main__":
    main()
