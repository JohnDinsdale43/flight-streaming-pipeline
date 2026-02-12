"""
JSON streaming module.

Converts FlightRecord objects into newline-delimited JSON (NDJSON) and
provides both file-based and in-memory streaming capabilities.

NDJSON is the de-facto format for streaming JSON into Arrow / DuckDB
because each line is an independent JSON object that can be parsed
incrementally.
"""
from __future__ import annotations

import json
import io
from datetime import datetime
from pathlib import Path
from typing import IO, Iterator

from src.generators.models import FlightRecord


def _json_serializer(obj: object) -> str:
    """Custom JSON serializer for types not handled by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def record_to_json_line(record: FlightRecord) -> str:
    """Serialize a single FlightRecord to a JSON string (no trailing newline)."""
    return record.model_dump_json()


def records_to_ndjson(records: Iterator[FlightRecord] | list[FlightRecord]) -> str:
    """Serialize an iterable of FlightRecords to an NDJSON string."""
    lines: list[str] = []
    for rec in records:
        lines.append(record_to_json_line(rec))
    return "\n".join(lines) + "\n" if lines else ""


def stream_to_file(
    records: Iterator[FlightRecord] | list[FlightRecord],
    path: str | Path,
) -> int:
    """
    Write records as NDJSON to *path*.

    Returns the number of records written.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(record_to_json_line(rec))
            fh.write("\n")
            count += 1
    return count


def stream_to_buffer(
    records: Iterator[FlightRecord] | list[FlightRecord],
) -> io.BytesIO:
    """
    Write records as NDJSON into an in-memory BytesIO buffer.

    The buffer is rewound to position 0 before returning so it can be
    read immediately by downstream consumers (e.g. Arrow JSON reader).
    """
    buf = io.BytesIO()
    for rec in records:
        line = record_to_json_line(rec) + "\n"
        buf.write(line.encode("utf-8"))
    buf.seek(0)
    return buf


def read_ndjson_file(path: str | Path) -> list[dict]:
    """Read an NDJSON file back into a list of dicts."""
    path = Path(path)
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def count_lines(path: str | Path) -> int:
    """Count non-empty lines in an NDJSON file."""
    path = Path(path)
    count = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count
