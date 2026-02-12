"""
Feature 5 — End-to-End Regression Tests
========================================

Full pipeline regression: generate -> stream -> ingest -> catalog -> query.

These tests validate the entire data flow and serve as the final
regression gate before any release.

Test matrix:
  5.1  Full pipeline — generate, stream to NDJSON, load via Arrow into
       DuckDB, write to Iceberg, query back
  5.2  Pipeline determinism — same seed produces identical Iceberg data
  5.3  Pipeline with large dataset — 500 records end-to-end
  5.4  DuckDB and Iceberg row counts match
  5.5  Query DuckDB after pipeline — aggregation queries work
  5.6  Query Iceberg after pipeline — scan returns correct data
  5.7  Multiple pipeline runs — Iceberg accumulates, DuckDB replaces
  5.8  Data integrity — spot-check specific field values survive pipeline
"""
from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pytest

from src.generators.flight_generator import FlightDataGenerator
from src.streaming.json_stream import stream_to_file, stream_to_buffer
from src.ingestion.arrow_loader import (
    ndjson_file_to_arrow,
    arrow_to_duckdb,
    load_ndjson_to_duckdb,
    query_flights,
)
from src.catalog.iceberg_catalog import (
    create_catalog,
    append_arrow_to_iceberg,
    scan_iceberg_table,
    list_snapshots,
    drop_warehouse,
)


pytestmark = pytest.mark.feature_5


# -----------------------------------------------------------------------
# Shared fixtures
# -----------------------------------------------------------------------
@pytest.fixture
def pipeline_env(tmp_path):
    """Set up a full pipeline environment."""
    base_time = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    gen = FlightDataGenerator(seed=42, base_time=base_time)
    ndjson_path = tmp_path / "flights.ndjson"
    warehouse_path = tmp_path / "iceberg_wh"
    con = duckdb.connect(":memory:")
    catalog = create_catalog(warehouse_path, catalog_name="e2e_catalog")

    yield {
        "generator": gen,
        "ndjson_path": ndjson_path,
        "warehouse_path": warehouse_path,
        "con": con,
        "catalog": catalog,
        "base_time": base_time,
    }

    con.close()
    drop_warehouse(warehouse_path)


def run_full_pipeline(env, n: int = 50) -> dict:
    """Execute the full pipeline and return metrics."""
    gen = env["generator"]
    ndjson_path = env["ndjson_path"]
    con = env["con"]
    catalog = env["catalog"]

    # 1. Generate
    records = gen.generate(n)

    # 2. Stream to NDJSON
    written = stream_to_file(records, ndjson_path)

    # 3. Load via Arrow into DuckDB
    arrow_table = ndjson_file_to_arrow(ndjson_path)
    duckdb_rows = arrow_to_duckdb(arrow_table, con, "flights")

    # 4. Write to Iceberg
    iceberg_rows = append_arrow_to_iceberg(catalog, arrow_table)

    return {
        "records": records,
        "written": written,
        "arrow_table": arrow_table,
        "duckdb_rows": duckdb_rows,
        "iceberg_rows": iceberg_rows,
    }


# -----------------------------------------------------------------------
# 5.1 Full pipeline
# -----------------------------------------------------------------------
class TestFullPipeline:
    def test_pipeline_completes_without_error(self, pipeline_env):
        result = run_full_pipeline(pipeline_env, n=20)
        assert result["written"] == 20
        assert result["duckdb_rows"] == 20
        assert result["iceberg_rows"] == 20

    def test_duckdb_queryable_after_pipeline(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=20)
        con = pipeline_env["con"]
        count = con.execute("SELECT count(*) FROM flights").fetchone()[0]
        assert count == 20

    def test_iceberg_scannable_after_pipeline(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=20)
        catalog = pipeline_env["catalog"]
        result = scan_iceberg_table(catalog)
        assert result.num_rows == 20


# -----------------------------------------------------------------------
# 5.2 Pipeline determinism
# -----------------------------------------------------------------------
class TestPipelineDeterminism:
    def test_same_seed_same_duckdb_data(self, tmp_path):
        results = []
        for i in range(2):
            base_time = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
            gen = FlightDataGenerator(seed=42, base_time=base_time)
            ndjson_path = tmp_path / f"flights_{i}.ndjson"
            con = duckdb.connect(":memory:")

            records = gen.generate(10)
            stream_to_file(records, ndjson_path)
            load_ndjson_to_duckdb(ndjson_path, con, "flights")
            rows = con.execute(
                "SELECT flight_id, airline_code, flight_number FROM flights ORDER BY flight_id"
            ).fetchall()
            results.append(rows)
            con.close()

        assert results[0] == results[1]


# -----------------------------------------------------------------------
# 5.3 Large dataset
# -----------------------------------------------------------------------
class TestLargeDataset:
    def test_500_records_end_to_end(self, pipeline_env):
        result = run_full_pipeline(pipeline_env, n=500)
        assert result["duckdb_rows"] == 500
        assert result["iceberg_rows"] == 500


# -----------------------------------------------------------------------
# 5.4 Row count consistency
# -----------------------------------------------------------------------
class TestRowCountConsistency:
    def test_duckdb_and_iceberg_match(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=100)
        con = pipeline_env["con"]
        catalog = pipeline_env["catalog"]

        duckdb_count = con.execute("SELECT count(*) FROM flights").fetchone()[0]
        iceberg_result = scan_iceberg_table(catalog)
        assert duckdb_count == iceberg_result.num_rows


# -----------------------------------------------------------------------
# 5.5 DuckDB aggregation queries
# -----------------------------------------------------------------------
class TestDuckdbAggregation:
    def test_count_by_status(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=200)
        con = pipeline_env["con"]
        result = con.execute(
            "SELECT status, count(*) as cnt FROM flights GROUP BY status ORDER BY cnt DESC"
        ).fetchall()
        total = sum(row[1] for row in result)
        assert total == 200

    def test_avg_delay(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=200)
        con = pipeline_env["con"]
        result = con.execute(
            "SELECT avg(delay_minutes) as avg_delay FROM flights"
        ).fetchone()
        assert result[0] is not None
        assert result[0] >= 0

    def test_count_by_flight_type(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=200)
        con = pipeline_env["con"]
        result = con.execute(
            "SELECT flight_type, count(*) as cnt FROM flights GROUP BY flight_type"
        ).fetchall()
        types = {row[0] for row in result}
        assert types.issubset({"arrival", "departure"})
        total = sum(row[1] for row in result)
        assert total == 200


# -----------------------------------------------------------------------
# 5.6 Iceberg query
# -----------------------------------------------------------------------
class TestIcebergQuery:
    def test_scan_returns_all_columns(self, pipeline_env):
        run_full_pipeline(pipeline_env, n=20)
        catalog = pipeline_env["catalog"]
        result = scan_iceberg_table(catalog)
        expected_cols = {
            "flight_id", "flight_type", "airline", "airline_code",
            "flight_number", "status", "delay_minutes",
        }
        assert expected_cols.issubset(set(result.column_names))


# -----------------------------------------------------------------------
# 5.7 Multiple pipeline runs
# -----------------------------------------------------------------------
class TestMultiplePipelineRuns:
    def test_iceberg_accumulates_duckdb_replaces(self, pipeline_env):
        # First run
        run_full_pipeline(pipeline_env, n=10)
        con = pipeline_env["con"]
        catalog = pipeline_env["catalog"]

        # Reset generator for second run with different seed
        pipeline_env["generator"] = FlightDataGenerator(
            seed=99, base_time=pipeline_env["base_time"]
        )
        run_full_pipeline(pipeline_env, n=15)

        # DuckDB should have 15 (replaced)
        duckdb_count = con.execute("SELECT count(*) FROM flights").fetchone()[0]
        assert duckdb_count == 15

        # Iceberg should have 25 (accumulated)
        iceberg_result = scan_iceberg_table(catalog)
        assert iceberg_result.num_rows == 25


# -----------------------------------------------------------------------
# 5.8 Data integrity spot-check
# -----------------------------------------------------------------------
class TestDataIntegrity:
    def test_specific_values_survive_pipeline(self, pipeline_env):
        result = run_full_pipeline(pipeline_env, n=5)
        records = result["records"]
        con = pipeline_env["con"]

        for rec in records:
            row = con.execute(
                f"SELECT airline_code, flight_number, status "
                f"FROM flights WHERE flight_id = '{rec.flight_id}'"
            ).fetchone()
            assert row is not None, f"Flight {rec.flight_id} not found in DuckDB"
            assert row[0] == rec.airline_code
            assert row[1] == rec.flight_number
            assert row[2] == rec.status.value
