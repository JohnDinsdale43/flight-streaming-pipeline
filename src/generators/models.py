"""
Pydantic models for flight data — mirrors the schema that NVIDIA NeMo
DataDesigner would produce for an airline arrivals/departures dataset.

DataDesigner (https://nvidia-nemo.github.io/DataDesigner/latest/) is used
as the *design pattern* for the synthetic data contract.  When an LLM
endpoint is available the generator can delegate to DataDesigner's
`generate()` API; otherwise we fall back to a deterministic Faker-based
generator that honours the same schema contract.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class FlightStatus(str, enum.Enum):
    SCHEDULED = "scheduled"
    BOARDING = "boarding"
    DEPARTED = "departed"
    IN_AIR = "in_air"
    LANDED = "landed"
    ARRIVED = "arrived"
    DELAYED = "delayed"
    CANCELLED = "cancelled"
    DIVERTED = "diverted"


class FlightType(str, enum.Enum):
    ARRIVAL = "arrival"
    DEPARTURE = "departure"


class FlightRecord(BaseModel):
    """Single flight arrival or departure record."""

    flight_id: str = Field(
        ..., description="Unique flight identifier, e.g. 'UA-1234'"
    )
    flight_type: FlightType = Field(
        ..., description="Whether this is an arrival or departure"
    )
    airline: str = Field(..., description="IATA airline name")
    airline_code: str = Field(
        ..., min_length=2, max_length=3, description="IATA 2-3 letter airline code"
    )
    flight_number: int = Field(
        ..., ge=1, le=9999, description="Numeric flight number"
    )
    origin_airport: str = Field(
        ..., min_length=3, max_length=4, description="IATA/ICAO origin airport code"
    )
    destination_airport: str = Field(
        ..., min_length=3, max_length=4, description="IATA/ICAO destination airport code"
    )
    scheduled_time: datetime = Field(
        ..., description="Scheduled departure/arrival time (UTC)"
    )
    estimated_time: Optional[datetime] = Field(
        None, description="Estimated departure/arrival time (UTC)"
    )
    actual_time: Optional[datetime] = Field(
        None, description="Actual departure/arrival time (UTC)"
    )
    status: FlightStatus = Field(
        ..., description="Current flight status"
    )
    gate: Optional[str] = Field(
        None, description="Gate assignment, e.g. 'B12'"
    )
    terminal: Optional[str] = Field(
        None, description="Terminal, e.g. 'T1'"
    )
    aircraft_type: Optional[str] = Field(
        None, description="Aircraft ICAO type designator, e.g. 'B738'"
    )
    delay_minutes: int = Field(
        default=0, ge=0, description="Delay in minutes (0 = on time)"
    )

    @field_validator("origin_airport", "destination_airport")
    @classmethod
    def uppercase_airport(cls, v: str) -> str:
        return v.upper()

    @field_validator("airline_code")
    @classmethod
    def uppercase_airline_code(cls, v: str) -> str:
        return v.upper()
