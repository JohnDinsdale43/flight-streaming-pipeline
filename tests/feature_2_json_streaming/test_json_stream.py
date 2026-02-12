"""
Feature 2 — JSON Streaming Regression Tests
============================================

Tests that FlightRecords serialize correctly to NDJSON and can be
written to files and in-memory buffers for downstream consumption.

Test matrix:
  2.1  Single record serialization — valid JSON line
  2.2  NDJSON string — correct line count, parseable
  2.3  File streaming — writes correct number of lines
  2.4  File round-trip — write then read back matches
  2.5  Buffer streaming — BytesIO contains valid NDJSON
  2.6  Empty input — produces empty output
  2.7  Large batch — 1000 records stream without error
  2.8  Line counting — count_lines matches record count
  2.9  Datetime serialization — ISO-8601 format preserved
  2.10 Unicode / special chars — airline names survive round-trip
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.generators.models import FlightRecord
from src.streaming.json_stream import (
    record_to_json_line,
    records_to_ndjson,
    stream_to_file,
    stream_to_buffer,
    read_ndjson_file,
    count_lines,
)


pytestmark = pytest.mark.feature_2


# -----------------------------------------------------------------------
# 2.1 Single record serialization
# -----------------------------------------------------------------------
class TestSingleRecordSerialization:
    def test_produces_valid_json(self, small_records):
        rec = small_records[0]
        line = record_to_json_line(rec)
        parsed = json.loads(line)
        assert isinstance(parsed, dict)
        assert parsed["flight_id"] == rec.flight_id

    def test_no_newlines_in_line(self, small_records):
        rec = small_records[0]
        line = record_to_json_line(rec)
        assert "\n" not in line


# -----------------------------------------------------------------------
# 2.2 NDJSON string
# -----------------------------------------------------------------------
class TestNdjsonString:
    def test_correct_line_count(self, small_records):
        ndjson = records_to_ndjson(small_records)
        lines = [l for l in ndjson.strip().split("\n") if l.strip()]
        assert len(lines) == len(small_records)

    def test_each_line_is_valid_json(self, small_records):
        ndjson = records_to_ndjson(small_records)
        for line in ndjson.strip().split("\n"):
            if line.strip():
                json.loads(line)  # should not raise


# -----------------------------------------------------------------------
# 2.3 File streaming
# -----------------------------------------------------------------------
class TestFileStreaming:
    def test_writes_correct_count(self, small_records, ndjson_path):
        count = stream_to_file(small_records, ndjson_path)
        assert count == len(small_records)

    def test_file_exists_after_write(self, small_records, ndjson_path):
        stream_to_file(small_records, ndjson_path)
        assert ndjson_path.exists()

    def test_file_has_correct_lines(self, small_records, ndjson_path):
        stream_to_file(small_records, ndjson_path)
        assert count_lines(ndjson_path) == len(small_records)


# -----------------------------------------------------------------------
# 2.4 File round-trip
# -----------------------------------------------------------------------
class TestFileRoundTrip:
    def test_round_trip_preserves_data(self, small_records, ndjson_path):
        stream_to_file(small_records, ndjson_path)
        loaded = read_ndjson_file(ndjson_path)
        assert len(loaded) == len(small_records)
        for orig, loaded_dict in zip(small_records, loaded):
            assert loaded_dict["flight_id"] == orig.flight_id
            assert loaded_dict["airline_code"] == orig.airline_code
            assert loaded_dict["flight_number"] == orig.flight_number

    def test_round_trip_preserves_all_fields(self, small_records, ndjson_path):
        stream_to_file(small_records, ndjson_path)
        loaded = read_ndjson_file(ndjson_path)
        orig_keys = set(small_records[0].model_dump().keys())
        for d in loaded:
            assert set(d.keys()) == orig_keys


# -----------------------------------------------------------------------
# 2.5 Buffer streaming
# -----------------------------------------------------------------------
class TestBufferStreaming:
    def test_buffer_contains_ndjson(self, small_records):
        buf = stream_to_buffer(small_records)
        content = buf.read().decode("utf-8")
        lines = [l for l in content.strip().split("\n") if l.strip()]
        assert len(lines) == len(small_records)

    def test_buffer_position_at_zero(self, small_records):
        buf = stream_to_buffer(small_records)
        assert buf.tell() == 0  # rewound for downstream readers


# -----------------------------------------------------------------------
# 2.6 Empty input
# -----------------------------------------------------------------------
class TestEmptyInput:
    def test_ndjson_empty_string(self):
        assert records_to_ndjson([]) == ""

    def test_file_empty(self, ndjson_path):
        count = stream_to_file([], ndjson_path)
        assert count == 0
        assert ndjson_path.exists()
        assert count_lines(ndjson_path) == 0

    def test_buffer_empty(self):
        buf = stream_to_buffer([])
        assert buf.read() == b""


# -----------------------------------------------------------------------
# 2.7 Large batch
# -----------------------------------------------------------------------
class TestLargeBatch:
    def test_1000_records_stream_to_file(self, generator, ndjson_path):
        records = generator.generate(1000)
        count = stream_to_file(records, ndjson_path)
        assert count == 1000
        assert count_lines(ndjson_path) == 1000


# -----------------------------------------------------------------------
# 2.8 Line counting
# -----------------------------------------------------------------------
class TestLineCounting:
    @pytest.mark.parametrize("n", [1, 10, 100])
    def test_count_matches_written(self, generator, ndjson_path, n):
        records = generator.generate(n)
        stream_to_file(records, ndjson_path)
        assert count_lines(ndjson_path) == n


# -----------------------------------------------------------------------
# 2.9 Datetime serialization
# -----------------------------------------------------------------------
class TestDatetimeSerialization:
    def test_scheduled_time_is_iso8601(self, small_records):
        rec = small_records[0]
        line = record_to_json_line(rec)
        parsed = json.loads(line)
        # Should parse back as ISO-8601
        from datetime import datetime
        dt = datetime.fromisoformat(parsed["scheduled_time"])
        assert dt is not None


# -----------------------------------------------------------------------
# 2.10 Special characters
# -----------------------------------------------------------------------
class TestSpecialCharacters:
    def test_airline_names_survive_roundtrip(self, small_records, ndjson_path):
        stream_to_file(small_records, ndjson_path)
        loaded = read_ndjson_file(ndjson_path)
        for orig, loaded_dict in zip(small_records, loaded):
            assert loaded_dict["airline"] == orig.airline
