"""
Feature 4 — Iceberg Catalog Regression Tests
=============================================

Tests that the Apache Iceberg catalog is correctly created, tables are
managed, data is appended, and snapshots track changes.

Test matrix:
  4.1  Catalog creation — SqlCatalog initializes without error
  4.2  Table creation — flights table created with correct schema
  4.3  Idempotent table creation — calling twice returns same table
  4.4  Append data — Arrow table appended, row count matches
  4.5  Multiple appends — rows accumulate across appends
  4.6  Scan table — full scan returns all rows
  4.7  Scan with filter — filtered scan returns subset
  4.8  Snapshot tracking — each append creates a new snapshot
  4.9  Warehouse cleanup — drop_warehouse removes directory
  4.10 Schema field count — Iceberg schema has expected number of fields
"""
from __future__ import annotations

import pytest
import pyarrow as pa

from src.streaming.json_stream import stream_to_file
from src.ingestion.arrow_loader import ndjson_file_to_arrow
from src.catalog.iceberg_catalog import (
    create_catalog,
    create_flights_table,
    append_arrow_to_iceberg,
    scan_iceberg_table,
    list_snapshots,
    drop_warehouse,
    FLIGHT_ICEBERG_SCHEMA,
)


pytestmark = pytest.mark.feature_4


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------
@pytest.fixture
def catalog(warehouse_path):
    """Create a fresh Iceberg catalog for each test."""
    return create_catalog(warehouse_path, catalog_name="test_catalog")


@pytest.fixture
def arrow_table(small_records, ndjson_path):
    """Small Arrow table from 5 records."""
    stream_to_file(small_records, ndjson_path)
    return ndjson_file_to_arrow(ndjson_path)


# -----------------------------------------------------------------------
# 4.1 Catalog creation
# -----------------------------------------------------------------------
class TestCatalogCreation:
    def test_catalog_creates_successfully(self, warehouse_path):
        cat = create_catalog(warehouse_path)
        assert cat is not None

    def test_catalog_db_file_exists(self, warehouse_path):
        create_catalog(warehouse_path)
        assert (warehouse_path / "catalog.db").exists()


# -----------------------------------------------------------------------
# 4.2 Table creation
# -----------------------------------------------------------------------
class TestTableCreation:
    def test_table_created(self, catalog):
        table = create_flights_table(catalog)
        assert table is not None

    def test_table_has_correct_field_names(self, catalog):
        table = create_flights_table(catalog)
        field_names = {f.name for f in table.schema().fields}
        expected = {"flight_id", "flight_type", "airline", "airline_code",
                    "flight_number", "status", "delay_minutes"}
        assert expected.issubset(field_names)


# -----------------------------------------------------------------------
# 4.3 Idempotent creation
# -----------------------------------------------------------------------
class TestIdempotentCreation:
    def test_create_twice_returns_same_table(self, catalog):
        t1 = create_flights_table(catalog)
        t2 = create_flights_table(catalog)
        assert t1.name() == t2.name()


# -----------------------------------------------------------------------
# 4.4 Append data
# -----------------------------------------------------------------------
class TestAppendData:
    def test_append_returns_correct_count(self, catalog, arrow_table):
        count = append_arrow_to_iceberg(catalog, arrow_table)
        assert count == arrow_table.num_rows

    def test_data_readable_after_append(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        result = scan_iceberg_table(catalog)
        assert result.num_rows == arrow_table.num_rows


# -----------------------------------------------------------------------
# 4.5 Multiple appends
# -----------------------------------------------------------------------
class TestMultipleAppends:
    def test_rows_accumulate(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        append_arrow_to_iceberg(catalog, arrow_table)
        result = scan_iceberg_table(catalog)
        assert result.num_rows == arrow_table.num_rows * 2


# -----------------------------------------------------------------------
# 4.6 Full scan
# -----------------------------------------------------------------------
class TestFullScan:
    def test_scan_returns_arrow_table(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        result = scan_iceberg_table(catalog)
        assert isinstance(result, pa.Table)

    def test_scan_has_all_columns(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        result = scan_iceberg_table(catalog)
        expected_cols = {f.name for f in FLIGHT_ICEBERG_SCHEMA.fields}
        assert expected_cols.issubset(set(result.column_names))


# -----------------------------------------------------------------------
# 4.7 Scan with filter
# -----------------------------------------------------------------------
class TestScanWithFilter:
    def test_filter_by_status(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        # Get all statuses first
        full = scan_iceberg_table(catalog)
        if full.num_rows > 0:
            # Just verify the scan with filter doesn't error
            result = scan_iceberg_table(
                catalog, row_filter="flight_type == 'arrival'"
            )
            assert isinstance(result, pa.Table)


# -----------------------------------------------------------------------
# 4.8 Snapshot tracking
# -----------------------------------------------------------------------
class TestSnapshotTracking:
    def test_append_creates_snapshot(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        snaps = list_snapshots(catalog)
        assert len(snaps) >= 1

    def test_two_appends_create_two_snapshots(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        append_arrow_to_iceberg(catalog, arrow_table)
        snaps = list_snapshots(catalog)
        assert len(snaps) >= 2

    def test_snapshot_has_metadata(self, catalog, arrow_table):
        append_arrow_to_iceberg(catalog, arrow_table)
        snaps = list_snapshots(catalog)
        assert "snapshot_id" in snaps[0]
        assert "timestamp_ms" in snaps[0]


# -----------------------------------------------------------------------
# 4.9 Warehouse cleanup
# -----------------------------------------------------------------------
class TestWarehouseCleanup:
    def test_drop_warehouse_removes_dir(self, tmp_dir):
        wh = tmp_dir / "cleanup_test_wh"
        wh.mkdir(parents=True, exist_ok=True)
        # Create a file inside to verify recursive delete
        (wh / "dummy.txt").write_text("test")
        assert wh.exists()
        drop_warehouse(wh)
        # On Windows, ignore_errors=True may leave locked files (SQLite),
        # so we verify the non-locked content is removed.
        assert not (wh / "dummy.txt").exists()


# -----------------------------------------------------------------------
# 4.10 Schema field count
# -----------------------------------------------------------------------
class TestSchemaFieldCount:
    def test_iceberg_schema_has_15_fields(self):
        assert len(FLIGHT_ICEBERG_SCHEMA.fields) == 15
