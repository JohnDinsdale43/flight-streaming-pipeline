"""
Feature 1 — Data Generation Regression Tests
=============================================

Tests that the synthetic flight data generator produces valid, deterministic
data conforming to the FlightRecord schema (DataDesigner contract).

Test matrix:
  1.1  Schema validation — every record passes Pydantic validation
  1.2  Determinism — same seed produces identical output
  1.3  Batch generation — generate(n) returns exactly n records
  1.4  Streaming generation — stream(n) yields exactly n records
  1.5  Field constraints — airport codes uppercase, flight_number in range
  1.6  Status distribution — all statuses appear given enough records
  1.7  Delay logic — delayed flights have delay_minutes > 0
  1.8  Airport pair uniqueness — origin != destination
  1.9  Edge case — generate(0) returns empty list
  1.10 Edge case — generate(1) returns single valid record
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from src.generators.flight_generator import FlightDataGenerator, AIRPORTS, AIRLINES
from src.generators.models import FlightRecord, FlightStatus, FlightType


pytestmark = pytest.mark.feature_1


# -----------------------------------------------------------------------
# 1.1 Schema validation
# -----------------------------------------------------------------------
class TestSchemaValidation:
    def test_all_records_are_flight_records(self, sample_records):
        for rec in sample_records:
            assert isinstance(rec, FlightRecord)

    def test_required_fields_present(self, sample_records):
        required = {
            "flight_id", "flight_type", "airline", "airline_code",
            "flight_number", "origin_airport", "destination_airport",
            "scheduled_time", "status", "delay_minutes",
        }
        for rec in sample_records:
            data = rec.model_dump()
            for field in required:
                assert field in data, f"Missing required field: {field}"
                assert data[field] is not None, f"Required field is None: {field}"

    def test_flight_type_is_valid_enum(self, sample_records):
        for rec in sample_records:
            assert rec.flight_type in FlightType

    def test_status_is_valid_enum(self, sample_records):
        for rec in sample_records:
            assert rec.status in FlightStatus


# -----------------------------------------------------------------------
# 1.2 Determinism
# -----------------------------------------------------------------------
class TestDeterminism:
    def test_same_seed_same_output(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        gen_a = FlightDataGenerator(seed=99, base_time=base)
        gen_b = FlightDataGenerator(seed=99, base_time=base)
        records_a = gen_a.generate(20)
        records_b = gen_b.generate(20)
        for a, b in zip(records_a, records_b):
            assert a.model_dump() == b.model_dump()

    def test_different_seed_different_output(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        gen_a = FlightDataGenerator(seed=1, base_time=base)
        gen_b = FlightDataGenerator(seed=2, base_time=base)
        records_a = gen_a.generate(10)
        records_b = gen_b.generate(10)
        # At least some records should differ
        diffs = sum(
            1 for a, b in zip(records_a, records_b)
            if a.flight_id != b.flight_id
        )
        assert diffs > 0


# -----------------------------------------------------------------------
# 1.3 Batch generation count
# -----------------------------------------------------------------------
class TestBatchGeneration:
    @pytest.mark.parametrize("n", [0, 1, 5, 50, 200])
    def test_generate_returns_exact_count(self, n):
        gen = FlightDataGenerator(seed=42)
        records = gen.generate(n)
        assert len(records) == n


# -----------------------------------------------------------------------
# 1.4 Streaming generation count
# -----------------------------------------------------------------------
class TestStreamingGeneration:
    def test_stream_yields_exact_count(self, generator):
        records = list(generator.stream(30))
        assert len(records) == 30

    def test_stream_yields_valid_records(self, generator):
        for rec in generator.stream(10):
            assert isinstance(rec, FlightRecord)


# -----------------------------------------------------------------------
# 1.5 Field constraints
# -----------------------------------------------------------------------
class TestFieldConstraints:
    def test_airport_codes_uppercase(self, sample_records):
        for rec in sample_records:
            assert rec.origin_airport == rec.origin_airport.upper()
            assert rec.destination_airport == rec.destination_airport.upper()

    def test_airline_code_uppercase(self, sample_records):
        for rec in sample_records:
            assert rec.airline_code == rec.airline_code.upper()

    def test_airline_code_length(self, sample_records):
        for rec in sample_records:
            assert 2 <= len(rec.airline_code) <= 3

    def test_flight_number_range(self, sample_records):
        for rec in sample_records:
            assert 1 <= rec.flight_number <= 9999

    def test_airport_codes_in_pool(self, sample_records):
        for rec in sample_records:
            assert rec.origin_airport in AIRPORTS
            assert rec.destination_airport in AIRPORTS

    def test_delay_minutes_non_negative(self, sample_records):
        for rec in sample_records:
            assert rec.delay_minutes >= 0


# -----------------------------------------------------------------------
# 1.6 Status distribution
# -----------------------------------------------------------------------
class TestStatusDistribution:
    def test_all_statuses_appear(self):
        """With 2000 records every status should appear at least once."""
        gen = FlightDataGenerator(seed=42)
        records = gen.generate(2000)
        seen = {rec.status for rec in records}
        for status in FlightStatus:
            assert status in seen, f"Status {status} never appeared"


# -----------------------------------------------------------------------
# 1.7 Delay logic
# -----------------------------------------------------------------------
class TestDelayLogic:
    def test_delayed_flights_have_positive_delay(self):
        gen = FlightDataGenerator(seed=42)
        records = gen.generate(500)
        delayed = [r for r in records if r.status == FlightStatus.DELAYED]
        assert len(delayed) > 0, "No delayed flights generated"
        for rec in delayed:
            assert rec.delay_minutes > 0

    def test_cancelled_flights_have_zero_delay(self):
        gen = FlightDataGenerator(seed=42)
        records = gen.generate(500)
        cancelled = [r for r in records if r.status == FlightStatus.CANCELLED]
        for rec in cancelled:
            assert rec.delay_minutes == 0


# -----------------------------------------------------------------------
# 1.8 Airport pair uniqueness
# -----------------------------------------------------------------------
class TestAirportPairs:
    def test_origin_differs_from_destination(self, sample_records):
        for rec in sample_records:
            assert rec.origin_airport != rec.destination_airport


# -----------------------------------------------------------------------
# 1.9 / 1.10 Edge cases
# -----------------------------------------------------------------------
class TestEdgeCases:
    def test_generate_zero(self):
        gen = FlightDataGenerator(seed=42)
        assert gen.generate(0) == []

    def test_generate_one(self):
        gen = FlightDataGenerator(seed=42)
        records = gen.generate(1)
        assert len(records) == 1
        assert isinstance(records[0], FlightRecord)
