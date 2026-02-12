"""
Apache Arrow ingestion module.

Loads NDJSON (newline-delimited JSON) data into DuckDB via PyArrow,
providing zero-copy transfer where possible.

Pipeline:  NDJSON file/buffer  -->  PyArrow Table  -->  DuckDB Table
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.json as paj


# ---------------------------------------------------------------------------
# Arrow schema matching FlightRecord
# ---------------------------------------------------------------------------

FLIGHT_ARROW_SCHEMA = pa.schema([
    pa.field("flight_id", pa.string(), nullable=False),
    pa.field("flight_type", pa.string(), nullable=False),
    pa.field("airline", pa.string(), nullable=False),
    pa.field("airline_code", pa.string(), nullable=False),
    pa.field("flight_number", pa.int32(), nullable=False),
    pa.field("origin_airport", pa.string(), nullable=False),
    pa.field("destination_airport", pa.string(), nullable=False),
    pa.field("scheduled_time", pa.string(), nullable=False),   # ISO-8601
    pa.field("estimated_time", pa.string(), nullable=True),
    pa.field("actual_time", pa.string(), nullable=True),
    pa.field("status", pa.string(), nullable=False),
    pa.field("gate", pa.string(), nullable=True),
    pa.field("terminal", pa.string(), nullable=True),
    pa.field("aircraft_type", pa.string(), nullable=True),
    pa.field("delay_minutes", pa.int32(), nullable=False),
])


def ndjson_file_to_arrow(path: str | Path) -> pa.Table:
    """Read an NDJSON file into a PyArrow Table."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"NDJSON file not found: {path}")
    table = paj.read_json(str(path))
    return table


def ndjson_buffer_to_arrow(buf: bytes | bytearray) -> pa.Table:
    """Read NDJSON bytes into a PyArrow Table."""
    reader = pa.BufferReader(buf)
    table = paj.read_json(reader)
    return table


def arrow_to_duckdb(
    table: pa.Table,
    con: duckdb.DuckDBPyConnection,
    table_name: str = "flights",
    mode: str = "create_or_replace",
) -> int:
    """
    Load a PyArrow Table into a DuckDB table.

    Parameters
    ----------
    table : pa.Table
        Source Arrow table.
    con : duckdb.DuckDBPyConnection
        DuckDB connection.
    table_name : str
        Target table name in DuckDB.
    mode : str
        'create_or_replace' — DROP + CREATE (default).
        'append' — INSERT INTO existing table.

    Returns
    -------
    int
        Number of rows loaded.
    """
    # Register the Arrow table as a virtual DuckDB table for zero-copy access
    _view_name = f"__arrow_{table_name}"
    con.register(_view_name, table)

    if mode == "create_or_replace":
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {_view_name}")
    elif mode == "append":
        con.execute(f"INSERT INTO {table_name} SELECT * FROM {_view_name}")
    else:
        con.unregister(_view_name)
        raise ValueError(f"Unknown mode: {mode!r}")

    con.unregister(_view_name)

    row_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    return row_count


def load_ndjson_to_duckdb(
    ndjson_path: str | Path,
    con: duckdb.DuckDBPyConnection,
    table_name: str = "flights",
) -> int:
    """
    Convenience: NDJSON file -> Arrow -> DuckDB in one call.

    Returns the number of rows loaded.
    """
    arrow_table = ndjson_file_to_arrow(ndjson_path)
    return arrow_to_duckdb(arrow_table, con, table_name)


def get_duckdb_table_schema(
    con: duckdb.DuckDBPyConnection,
    table_name: str = "flights",
) -> list[tuple[str, str]]:
    """Return list of (column_name, column_type) for a DuckDB table."""
    rows = con.execute(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
    ).fetchall()
    return rows


def query_flights(
    con: duckdb.DuckDBPyConnection,
    table_name: str = "flights",
    where: Optional[str] = None,
    limit: int = 100,
) -> pa.Table:
    """Run a query against the flights table and return an Arrow table."""
    sql = f"SELECT * FROM {table_name}"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {limit}"
    return con.execute(sql).fetch_arrow_table()
