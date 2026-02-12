"""
Apache Iceberg catalog module.

Creates and manages an Iceberg catalog backed by a local filesystem
warehouse, using PyIceberg with FsspecFileIO for Windows compatibility.

Architecture:
    DuckDB (Arrow Table) --> Iceberg Table (Parquet files in warehouse/)
                         <-- Query via PyIceberg scan -> Arrow
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Optional

import pyarrow as pa

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
)


# ---------------------------------------------------------------------------
# Iceberg schema matching FlightRecord
# ---------------------------------------------------------------------------
# All fields are optional (required=False) so that standard Arrow tables
# (which default to nullable) can be appended without schema mismatch.
# flight_number and delay_minutes use LongType because Arrow/DuckDB JSON
# readers produce int64 by default.

FLIGHT_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="flight_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="flight_type", field_type=StringType(), required=False),
    NestedField(field_id=3, name="airline", field_type=StringType(), required=False),
    NestedField(field_id=4, name="airline_code", field_type=StringType(), required=False),
    NestedField(field_id=5, name="flight_number", field_type=LongType(), required=False),
    NestedField(field_id=6, name="origin_airport", field_type=StringType(), required=False),
    NestedField(field_id=7, name="destination_airport", field_type=StringType(), required=False),
    NestedField(field_id=8, name="scheduled_time", field_type=StringType(), required=False),
    NestedField(field_id=9, name="estimated_time", field_type=StringType(), required=False),
    NestedField(field_id=10, name="actual_time", field_type=StringType(), required=False),
    NestedField(field_id=11, name="status", field_type=StringType(), required=False),
    NestedField(field_id=12, name="gate", field_type=StringType(), required=False),
    NestedField(field_id=13, name="terminal", field_type=StringType(), required=False),
    NestedField(field_id=14, name="aircraft_type", field_type=StringType(), required=False),
    NestedField(field_id=15, name="delay_minutes", field_type=LongType(), required=False),
)


def _warehouse_uri(warehouse_path: Path) -> str:
    """
    Convert a local path to a URI suitable for PyIceberg.

    On Windows, paths like ``C:\\Users\\...`` must be converted to
    ``file:///C:/Users/...`` so PyIceberg's IO layer resolves them
    correctly.
    """
    return warehouse_path.as_uri()


def create_catalog(
    warehouse_path: str | Path,
    catalog_name: str = "flight_catalog",
) -> SqlCatalog:
    """
    Create (or connect to) a SQLite-backed Iceberg catalog.

    Uses ``FsspecFileIO`` for cross-platform (Windows) compatibility.
    """
    warehouse_path = Path(warehouse_path).resolve()
    warehouse_path.mkdir(parents=True, exist_ok=True)

    db_path = str(warehouse_path / "catalog.db").replace(os.sep, "/")

    catalog = SqlCatalog(
        catalog_name,
        **{
            "uri": f"sqlite:///{db_path}",
            "warehouse": _warehouse_uri(warehouse_path),
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )
    return catalog


def create_flights_table(
    catalog: SqlCatalog,
    namespace: str = "flights_db",
    table_name: str = "flights",
):
    """
    Create the flights Iceberg table if it does not exist.

    Returns the PyIceberg Table handle.
    """
    # Ensure namespace exists
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass  # namespace already exists

    full_name = f"{namespace}.{table_name}"

    try:
        return catalog.load_table(full_name)
    except Exception:
        return catalog.create_table(
            identifier=full_name,
            schema=FLIGHT_ICEBERG_SCHEMA,
        )


def _coerce_arrow_schema(arrow_table: pa.Table) -> pa.Table:
    """
    Coerce an Arrow table's schema to match the Iceberg schema expectations.

    - Cast int32 columns to int64 (Iceberg LongType).
    - Ensure all string columns use ``pa.string()`` (not ``large_string``).
    - Cast ``pa.null()`` columns to ``pa.string()`` (all-null columns from
      JSON parsing, e.g. ``estimated_time`` when every value is null).
    """
    # Build a target-type lookup from the Iceberg schema
    _ICEBERG_TYPE_MAP: dict[str, pa.DataType] = {}
    for f in FLIGHT_ICEBERG_SCHEMA.fields:
        if isinstance(f.field_type, LongType):
            _ICEBERG_TYPE_MAP[f.name] = pa.int64()
        else:
            _ICEBERG_TYPE_MAP[f.name] = pa.string()

    new_fields = []
    new_columns = []
    for i, field in enumerate(arrow_table.schema):
        col = arrow_table.column(i)
        target_type = _ICEBERG_TYPE_MAP.get(field.name)

        if target_type and col.type != target_type:
            # Handle null-type columns (all values null)
            if col.type == pa.null():
                col = pa.array([None] * len(col), type=target_type)
            else:
                col = col.cast(target_type)
            field = field.with_type(target_type)

        new_fields.append(field)
        new_columns.append(col)
    return pa.table(
        {f.name: c for f, c in zip(new_fields, new_columns)},
    )


def append_arrow_to_iceberg(
    catalog: SqlCatalog,
    arrow_table: pa.Table,
    namespace: str = "flights_db",
    table_name: str = "flights",
) -> int:
    """
    Append an Arrow table to the Iceberg flights table.

    Returns the number of rows appended.
    """
    ice_table = create_flights_table(catalog, namespace, table_name)
    coerced = _coerce_arrow_schema(arrow_table)
    ice_table.append(coerced)
    return arrow_table.num_rows


def scan_iceberg_table(
    catalog: SqlCatalog,
    namespace: str = "flights_db",
    table_name: str = "flights",
    row_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> pa.Table:
    """
    Scan the Iceberg table and return an Arrow table.

    Parameters
    ----------
    row_filter : str | None
        Iceberg expression string, e.g. ``"status == 'delayed'"``.
    limit : int | None
        Max rows to return.
    """
    full_name = f"{namespace}.{table_name}"
    ice_table = catalog.load_table(full_name)
    scan = ice_table.scan()
    if row_filter:
        scan = scan.filter(row_filter)
    if limit:
        scan = scan.limit(limit)
    return scan.to_arrow()


def list_snapshots(
    catalog: SqlCatalog,
    namespace: str = "flights_db",
    table_name: str = "flights",
) -> list[dict]:
    """Return metadata about all snapshots for the table."""
    full_name = f"{namespace}.{table_name}"
    ice_table = catalog.load_table(full_name)
    snapshots = []
    for snap in ice_table.metadata.snapshots:
        snapshots.append({
            "snapshot_id": snap.snapshot_id,
            "timestamp_ms": snap.timestamp_ms,
            "operation": snap.summary.operation if snap.summary else None,
        })
    return snapshots


def drop_warehouse(warehouse_path: str | Path) -> None:
    """Remove the entire warehouse directory (for test cleanup)."""
    warehouse_path = Path(warehouse_path)
    if warehouse_path.exists():
        shutil.rmtree(warehouse_path, ignore_errors=True)
