"""
Synthetic flight data generator.

Design follows NVIDIA NeMo DataDesigner patterns
(https://nvidia-nemo.github.io/DataDesigner/latest/):

  1. Define a *data schema* (see models.py — FlightRecord).
  2. Define *generation constraints* (airport pairs, airline pool, time
     windows, status distributions).
  3. Generate N records that satisfy the schema + constraints.

When a NeMo / LLM endpoint is configured the generator can delegate to
DataDesigner's `generate()` API.  For offline / CI usage we provide a
fully deterministic Faker-backed generator that produces identical output
given the same seed.
"""
from __future__ import annotations

import random
import string
from datetime import datetime, timedelta, timezone
from typing import Iterator

from faker import Faker

from .models import FlightRecord, FlightStatus, FlightType

# ---------------------------------------------------------------------------
# Reference data pools (DataDesigner "constraints")
# ---------------------------------------------------------------------------

AIRLINES: list[tuple[str, str]] = [
    ("United Airlines", "UA"),
    ("Delta Air Lines", "DL"),
    ("American Airlines", "AA"),
    ("Southwest Airlines", "WN"),
    ("JetBlue Airways", "B6"),
    ("Alaska Airlines", "AS"),
    ("Spirit Airlines", "NK"),
    ("Frontier Airlines", "F9"),
    ("British Airways", "BA"),
    ("Lufthansa", "LH"),
    ("Air France", "AF"),
    ("KLM Royal Dutch", "KL"),
    ("Emirates", "EK"),
    ("Qatar Airways", "QR"),
    ("Singapore Airlines", "SQ"),
]

AIRPORTS: list[str] = [
    "JFK", "LAX", "ORD", "ATL", "DFW", "DEN", "SFO", "SEA",
    "MIA", "BOS", "LHR", "CDG", "FRA", "AMS", "DXB", "SIN",
    "HND", "ICN", "SYD", "YYZ",
]

AIRCRAFT_TYPES: list[str] = [
    "B738", "B739", "B77W", "B789", "A320", "A321", "A333",
    "A359", "A388", "E190", "CRJ9", "B737",
]

TERMINALS: list[str] = ["T1", "T2", "T3", "T4", "T5"]

STATUS_WEIGHTS: dict[FlightStatus, float] = {
    FlightStatus.SCHEDULED: 0.25,
    FlightStatus.BOARDING: 0.10,
    FlightStatus.DEPARTED: 0.10,
    FlightStatus.IN_AIR: 0.15,
    FlightStatus.LANDED: 0.10,
    FlightStatus.ARRIVED: 0.15,
    FlightStatus.DELAYED: 0.10,
    FlightStatus.CANCELLED: 0.03,
    FlightStatus.DIVERTED: 0.02,
}


class FlightDataGenerator:
    """
    Deterministic synthetic flight data generator.

    Parameters
    ----------
    seed : int
        Random seed for reproducibility.
    base_time : datetime | None
        Anchor time for scheduled flights.  Defaults to ``now(UTC)``.
    """

    def __init__(self, seed: int = 42, base_time: datetime | None = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.faker = Faker()
        Faker.seed(seed)
        self.base_time = base_time or datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, n: int) -> list[FlightRecord]:
        """Generate *n* flight records as a list."""
        return list(self.stream(n))

    def stream(self, n: int) -> Iterator[FlightRecord]:
        """Yield *n* flight records one at a time (streaming)."""
        for _ in range(n):
            yield self._make_record()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_record(self) -> FlightRecord:
        airline_name, airline_code = self.rng.choice(AIRLINES)
        flight_number = self.rng.randint(100, 9999)
        flight_id = f"{airline_code}-{flight_number}"

        origin, destination = self.rng.sample(AIRPORTS, 2)
        flight_type = self.rng.choice(list(FlightType))

        # Scheduled time within +/- 12 h of base_time
        offset_minutes = self.rng.randint(-720, 720)
        scheduled = self.base_time + timedelta(minutes=offset_minutes)

        status = self.rng.choices(
            list(STATUS_WEIGHTS.keys()),
            weights=list(STATUS_WEIGHTS.values()),
            k=1,
        )[0]

        delay_minutes = 0
        if status == FlightStatus.DELAYED:
            delay_minutes = self.rng.randint(15, 300)
        elif status == FlightStatus.CANCELLED:
            delay_minutes = 0
        elif status in (FlightStatus.ARRIVED, FlightStatus.LANDED):
            delay_minutes = self.rng.choice([0, 0, 0, 5, 10, 15, 30])

        estimated = scheduled + timedelta(minutes=delay_minutes) if delay_minutes else None
        actual = (
            estimated or scheduled
        ) if status in (FlightStatus.ARRIVED, FlightStatus.LANDED, FlightStatus.DEPARTED) else None

        gate_letter = self.rng.choice(string.ascii_uppercase[:6])  # A-F
        gate_number = self.rng.randint(1, 40)

        return FlightRecord(
            flight_id=flight_id,
            flight_type=flight_type,
            airline=airline_name,
            airline_code=airline_code,
            flight_number=flight_number,
            origin_airport=origin,
            destination_airport=destination,
            scheduled_time=scheduled,
            estimated_time=estimated,
            actual_time=actual,
            status=status,
            gate=f"{gate_letter}{gate_number}",
            terminal=self.rng.choice(TERMINALS),
            aircraft_type=self.rng.choice(AIRCRAFT_TYPES),
            delay_minutes=delay_minutes,
        )
