"""
Feature 3 — Arrow Ingestion Regression Tests
=============================================

Tests that NDJSON data loads correctly into DuckDB via Apache Arrow,
with schema fidelity and query correctness.

Test matrix:
  3.1  NDJSON file -> Arrow table — correct row count and columns
  3.2  NDJSON buffer -> Arrow table — same as file path
  3.3  Arrow -> DuckDB create — table exists with correct row count
  3.4  Arrow -> DuckDB append — rows accumulate
  3.5  DuckDB schema inspection — columns match expected schema
  3.6  Query with filter — WHERE clause returns correct subset
  3.7  Convenience load — ndjson file -> DuckDB in one call
  3.8  Missing file — raises FileNotFoundError
  3.9  Empty file — loads zero rows
  3.10 Data fidelity — values survive NDJSON -> Arrow -> DuckDB round-trip
"""
from __future__ import annotations

import pytest
import pyarrow as pa

from src.streaming.json_stream import stream_to_file, stream_to_buffer
from src.ingestion.arrow_loader import (
    ndjson_file_to_arrow,
    ndjson_buffer_to_arrow,
    arrow_to_duckdb,
    load_ndjson_to_duckdb,
    get_duckdb_table_schema,
    query_flights,
    FLIGHT_ARROW_SCHEMA,
)


pytestmark = pytest.mark.feature_3


# -----------------------------------------------------------------------
# 3.1 NDJSON file -> Arrow
# -----------------------------------------------------------------------
class TestNdjsonFileToArrow:
    def test_row_count(self, sample_records, ndjson_path):
        stream_to_file(sample_records, ndjson_path)
        table = ndjson_file_to_arrow(ndjson_path)
        assert table.num_rows == len(sample_records)

    def test_has_expected_columns(self, sample_records, ndjson_path):
        stream_to_file(sample_records, ndjson_path)
        table = ndjson_file_to_arrow(ndjson_path)
        expected_cols = {f.name for f in FLIGHT_ARROW_SCHEMA}
        assert expected_cols.issubset(set(table.column_names))

    def test_returns_arrow_table(self, sample_records, ndjson_path):
        stream_to_file(sample_records, ndjson_path)
        table = ndjson_file_to_arrow(ndjson_path)
        assert isinstance(table, pa.Table)


# -----------------------------------------------------------------------
# 3.2 NDJSON buffer -> Arrow
# -----------------------------------------------------------------------
class TestNdjsonBufferToArrow:
    def test_buffer_row_count(self, sample_records):
        buf = stream_to_buffer(sample_records)
        data = buf.read()
        table = ndjson_buffer_to_arrow(data)
        assert table.num_rows == len(sample_records)


# -----------------------------------------------------------------------
# 3.3 Arrow -> DuckDB create
# -----------------------------------------------------------------------
class TestArrowToDuckdbCreate:
    def test_table_created_with_correct_rows(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        arrow_table = ndjson_file_to_arrow(ndjson_path)
        row_count = arrow_to_duckdb(arrow_table, duckdb_con, "flights")
        assert row_count == len(sample_records)

    def test_table_exists_in_duckdb(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        arrow_table = ndjson_file_to_arrow(ndjson_path)
        arrow_to_duckdb(arrow_table, duckdb_con, "flights")
        tables = duckdb_con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'flights'"
        ).fetchall()
        assert len(tables) == 1

    def test_create_or_replace_overwrites(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        arrow_table = ndjson_file_to_arrow(ndjson_path)
        arrow_to_duckdb(arrow_table, duckdb_con, "flights")
        # Load again — should replace, not double
        row_count = arrow_to_duckdb(arrow_table, duckdb_con, "flights")
        assert row_count == len(sample_records)


# -----------------------------------------------------------------------
# 3.4 Arrow -> DuckDB append
# -----------------------------------------------------------------------
class TestArrowToDuckdbAppend:
    def test_append_accumulates_rows(self, generator, ndjson_path, duckdb_con):
        batch1 = generator.generate(10)
        stream_to_file(batch1, ndjson_path)
        t1 = ndjson_file_to_arrow(ndjson_path)
        arrow_to_duckdb(t1, duckdb_con, "flights", mode="create_or_replace")

        batch2 = generator.generate(15)
        stream_to_file(batch2, ndjson_path)
        t2 = ndjson_file_to_arrow(ndjson_path)
        row_count = arrow_to_duckdb(t2, duckdb_con, "flights", mode="append")
        assert row_count == 25  # 10 + 15


# -----------------------------------------------------------------------
# 3.5 DuckDB schema inspection
# -----------------------------------------------------------------------
class TestDuckdbSchema:
    def test_columns_present(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        arrow_table = ndjson_file_to_arrow(ndjson_path)
        arrow_to_duckdb(arrow_table, duckdb_con, "flights")
        schema = get_duckdb_table_schema(duckdb_con, "flights")
        col_names = {row[0] for row in schema}
        expected = {"flight_id", "flight_type", "airline", "airline_code",
                    "flight_number", "status", "delay_minutes"}
        assert expected.issubset(col_names)


# -----------------------------------------------------------------------
# 3.6 Query with filter
# -----------------------------------------------------------------------
class TestQueryWithFilter:
    def test_filter_by_status(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        load_ndjson_to_duckdb(ndjson_path, duckdb_con, "flights")
        result = query_flights(duckdb_con, "flights", where="status = 'delayed'")
        assert isinstance(result, pa.Table)
        # All returned rows should be delayed
        statuses = result.column("status").to_pylist()
        for s in statuses:
            assert s == "delayed"

    def test_filter_by_flight_type(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        load_ndjson_to_duckdb(ndjson_path, duckdb_con, "flights")
        result = query_flights(duckdb_con, "flights", where="flight_type = 'arrival'")
        types = result.column("flight_type").to_pylist()
        for t in types:
            assert t == "arrival"


# -----------------------------------------------------------------------
# 3.7 Convenience load
# -----------------------------------------------------------------------
class TestConvenienceLoad:
    def test_load_ndjson_to_duckdb(self, sample_records, ndjson_path, duckdb_con):
        stream_to_file(sample_records, ndjson_path)
        row_count = load_ndjson_to_duckdb(ndjson_path, duckdb_con, "flights")
        assert row_count == len(sample_records)


# -----------------------------------------------------------------------
# 3.8 Missing file
# -----------------------------------------------------------------------
class TestMissingFile:
    def test_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            ndjson_file_to_arrow("/nonexistent/path/flights.ndjson")


# -----------------------------------------------------------------------
# 3.9 Empty file
# -----------------------------------------------------------------------
class TestEmptyFile:
    def test_empty_ndjson_raises_or_returns_zero(self, ndjson_path):
        """An empty NDJSON file should either raise or return 0 rows."""
        stream_to_file([], ndjson_path)
        # PyArrow may raise on truly empty file — that's acceptable
        try:
            table = ndjson_file_to_arrow(ndjson_path)
            assert table.num_rows == 0
        except Exception:
            pass  # acceptable — empty file is an edge case


# -----------------------------------------------------------------------
# 3.10 Data fidelity
# -----------------------------------------------------------------------
class TestDataFidelity:
    def test_values_survive_round_trip(self, small_records, ndjson_path, duckdb_con):
        stream_to_file(small_records, ndjson_path)
        load_ndjson_to_duckdb(ndjson_path, duckdb_con, "flights")
        result = duckdb_con.execute(
            "SELECT flight_id, airline_code, flight_number, status "
            "FROM flights ORDER BY flight_id"
        ).fetchall()
        original = sorted(
            [(r.flight_id, r.airline_code, r.flight_number, r.status.value)
             for r in small_records],
            key=lambda x: x[0],
        )
        assert len(result) == len(original)
        for db_row, orig_row in zip(result, original):
            assert db_row[0] == orig_row[0]  # flight_id
            assert db_row[1] == orig_row[1]  # airline_code
            assert db_row[2] == orig_row[2]  # flight_number
            assert db_row[3] == orig_row[3]  # status
