"""
Shared pytest fixtures used across all feature test packs.
"""
from __future__ import annotations

import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from src.generators.flight_generator import FlightDataGenerator


# ---------------------------------------------------------------------------
# Deterministic generator
# ---------------------------------------------------------------------------

@pytest.fixture
def generator() -> FlightDataGenerator:
    """Seeded generator for reproducible tests."""
    return FlightDataGenerator(
        seed=42,
        base_time=datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_records(generator):
    """50 deterministic flight records."""
    return generator.generate(50)


@pytest.fixture
def small_records(generator):
    """5 deterministic flight records (fast tests)."""
    return generator.generate(5)


# ---------------------------------------------------------------------------
# Temp directory
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a clean temporary directory per test."""
    return tmp_path


@pytest.fixture
def ndjson_path(tmp_dir) -> Path:
    """Path for a temporary NDJSON file."""
    return tmp_dir / "flights.ndjson"


# ---------------------------------------------------------------------------
# DuckDB in-memory connection
# ---------------------------------------------------------------------------

@pytest.fixture
def duckdb_con():
    """Fresh in-memory DuckDB connection per test."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


# ---------------------------------------------------------------------------
# Iceberg warehouse
# ---------------------------------------------------------------------------

@pytest.fixture
def warehouse_path(tmp_dir) -> Path:
    """Temporary Iceberg warehouse directory."""
    wh = tmp_dir / "iceberg_warehouse"
    wh.mkdir(parents=True, exist_ok=True)
    return wh
