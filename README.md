<p align="center">
  <img src="https://img.shields.io/badge/python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/tests-88%20passed-22c55e?style=for-the-badge&logo=pytest&logoColor=white" />
  <img src="https://img.shields.io/badge/coverage-98%25-22c55e?style=for-the-badge" />
  <img src="https://img.shields.io/badge/DuckDB-1.1%2B-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" />
  <img src="https://img.shields.io/badge/Apache%20Arrow-15%2B-E34F26?style=for-the-badge&logo=apache&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Iceberg-0.7%2B-4A90D9?style=for-the-badge&logo=apache&logoColor=white" />
</p>

# Flight Streaming Pipeline

> **DataDesigner &rarr; NDJSON Stream &rarr; Apache Arrow &rarr; DuckDB &rarr; Apache Iceberg**

A fully tested, end-to-end streaming pipeline that generates synthetic flight arrival and departure data following [NVIDIA NeMo DataDesigner](https://nvidia-nemo.github.io/DataDesigner/latest/) patterns, streams it as newline-delimited JSON, ingests via Apache Arrow zero-copy into DuckDB, and catalogs it in Apache Iceberg with snapshot-based version control.

Diagrams styled with [**beautiful-mermaid**](https://github.com/lukilabs/beautiful-mermaid).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Pipeline Flowchart](#pipeline-flowchart)
- [Flight State Machine](#flight-state-machine)
- [Sequence Diagram — Pipeline Execution](#sequence-diagram--pipeline-execution)
- [Class Diagram](#class-diagram)
- [Entity-Relationship Diagram](#entity-relationship-diagram)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [TDD Feature Breakdown](#tdd-feature-breakdown)
- [License](#license)

---

## Architecture Overview

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#6366f1', 'primaryTextColor': '#ffffff', 'primaryBorderColor': '#4f46e5', 'lineColor': '#a5b4fc', 'secondaryColor': '#f0abfc', 'tertiaryColor': '#e0e7ff', 'fontSize': '14px' }}}%%
graph LR
    subgraph " "
        direction LR
        A["🎲 <b>DataDesigner</b><br/>Synthetic Generator"]
        B["📄 <b>NDJSON</b><br/>JSON Stream"]
        C["🏹 <b>Apache Arrow</b><br/>Zero-Copy Table"]
        D["🦆 <b>DuckDB</b><br/>Analytical Queries"]
        E["🧊 <b>Apache Iceberg</b><br/>Versioned Catalog"]
    end

    A -->|"generate(n)"| B
    B -->|"stream_to_file()"| C
    C -->|"register() zero-copy"| D
    C -->|"append()"| E
    D -.->|"query"| E

    style A fill:#6366f1,stroke:#4f46e5,color:#fff,stroke-width:2px
    style B fill:#f59e0b,stroke:#d97706,color:#fff,stroke-width:2px
    style C fill:#ef4444,stroke:#dc2626,color:#fff,stroke-width:2px
    style D fill:#facc15,stroke:#eab308,color:#000,stroke-width:2px
    style E fill:#3b82f6,stroke:#2563eb,color:#fff,stroke-width:2px
```

---

## Pipeline Flowchart

The complete data flow from generation through to queryable Iceberg tables, including error handling and branching logic.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#6366f1', 'primaryTextColor': '#fff', 'primaryBorderColor': '#4f46e5', 'lineColor': '#a5b4fc', 'secondaryColor': '#fbbf24', 'tertiaryColor': '#e0e7ff', 'fontSize': '13px' }}}%%
flowchart TD
    START(["▶ Pipeline Start"]) --> SEED["Set random seed<br/><code>seed=42</code>"]
    SEED --> GEN["Instantiate<br/><b>FlightDataGenerator</b>"]
    GEN --> MODE{Batch or<br/>Stream?}

    MODE -->|"generate(n)"| BATCH["Collect <i>n</i> records<br/>into <code>list</code>"]
    MODE -->|"stream(n)"| STREAM["Yield records<br/>one-by-one via <code>Iterator</code>"]

    BATCH --> VALIDATE["Pydantic validation<br/><code>FlightRecord</code>"]
    STREAM --> VALIDATE

    VALIDATE --> NDJSON_CHOICE{Output<br/>target?}

    NDJSON_CHOICE -->|File| FILE_WRITE["<code>stream_to_file()</code><br/>Write .ndjson"]
    NDJSON_CHOICE -->|Memory| BUF_WRITE["<code>stream_to_buffer()</code><br/>Write BytesIO"]

    FILE_WRITE --> ARROW_READ["<code>ndjson_file_to_arrow()</code><br/>Parse → <b>pa.Table</b>"]
    BUF_WRITE --> ARROW_BUF["<code>ndjson_buffer_to_arrow()</code><br/>Parse → <b>pa.Table</b>"]

    ARROW_READ --> COERCE["<code>_coerce_arrow_schema()</code><br/>Cast null/int32 → int64/string"]
    ARROW_BUF --> COERCE

    COERCE --> LOAD_MODE{DuckDB<br/>load mode?}

    LOAD_MODE -->|"create_or_replace"| DUCK_CREATE["DROP + CREATE TABLE<br/>via <code>register()</code> zero-copy"]
    LOAD_MODE -->|"append"| DUCK_APPEND["INSERT INTO<br/>existing table"]

    DUCK_CREATE --> ICE_APPEND["<code>append_arrow_to_iceberg()</code><br/>Write Parquet to warehouse"]
    DUCK_APPEND --> ICE_APPEND

    ICE_APPEND --> SNAPSHOT["New Iceberg <b>snapshot</b><br/>created automatically"]
    SNAPSHOT --> QUERY_READY(["✅ Query Ready<br/>DuckDB SQL │ Iceberg Scan"])

    style START fill:#22c55e,stroke:#16a34a,color:#fff,stroke-width:2px
    style QUERY_READY fill:#22c55e,stroke:#16a34a,color:#fff,stroke-width:2px
    style MODE fill:#f59e0b,stroke:#d97706,color:#fff,stroke-width:2px
    style NDJSON_CHOICE fill:#f59e0b,stroke:#d97706,color:#fff,stroke-width:2px
    style LOAD_MODE fill:#f59e0b,stroke:#d97706,color:#fff,stroke-width:2px
    style VALIDATE fill:#8b5cf6,stroke:#7c3aed,color:#fff,stroke-width:2px
    style COERCE fill:#8b5cf6,stroke:#7c3aed,color:#fff,stroke-width:2px
    style SNAPSHOT fill:#3b82f6,stroke:#2563eb,color:#fff,stroke-width:2px
    style SEED fill:#6366f1,stroke:#4f46e5,color:#fff,stroke-width:2px
    style GEN fill:#6366f1,stroke:#4f46e5,color:#fff,stroke-width:2px
    style BATCH fill:#6366f1,stroke:#4f46e5,color:#fff,stroke-width:2px
    style STREAM fill:#6366f1,stroke:#4f46e5,color:#fff,stroke-width:2px
    style FILE_WRITE fill:#f59e0b,stroke:#d97706,color:#fff,stroke-width:2px
    style BUF_WRITE fill:#f59e0b,stroke:#d97706,color:#fff,stroke-width:2px
    style ARROW_READ fill:#ef4444,stroke:#dc2626,color:#fff,stroke-width:2px
    style ARROW_BUF fill:#ef4444,stroke:#dc2626,color:#fff,stroke-width:2px
    style DUCK_CREATE fill:#facc15,stroke:#eab308,color:#000,stroke-width:2px
    style DUCK_APPEND fill:#facc15,stroke:#eab308,color:#000,stroke-width:2px
    style ICE_APPEND fill:#3b82f6,stroke:#2563eb,color:#fff,stroke-width:2px
```

---

## Flight State Machine

Every `FlightRecord` has a `status` field governed by this state diagram. The generator uses weighted random selection across these states.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#6366f1', 'primaryTextColor': '#fff', 'primaryBorderColor': '#4f46e5', 'lineColor': '#a5b4fc', 'fontSize': '13px' }}}%%
stateDiagram-v2
    direction LR

    [*] --> Scheduled : Flight created

    Scheduled --> Boarding : Gate open
    Scheduled --> Delayed : Delay detected
    Scheduled --> Cancelled : Flight cancelled

    Boarding --> Departed : Doors closed

    Delayed --> Boarding : Delay resolved
    Delayed --> Cancelled : Cancelled after delay

    Departed --> InAir : Airborne
    Departed --> Diverted : Rerouted on ground

    InAir --> Landed : Touchdown
    InAir --> Diverted : Emergency / weather

    Landed --> Arrived : At gate

    Diverted --> Landed : Diverted landing

    Arrived --> [*]
    Cancelled --> [*]

    state Scheduled {
        direction LR
        [*] --> Waiting
        Waiting --> GateAssigned
    }

    note right of Scheduled
        Weight: 25%
    end note

    note right of Delayed
        Weight: 10%
        delay_minutes: 15–300
    end note

    note right of Cancelled
        Weight: 3%
        delay_minutes: 0
    end note

    note left of Diverted
        Weight: 2%
    end note
```

---

## Sequence Diagram — Pipeline Execution

The full interaction between components during a single pipeline run.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#6366f1', 'primaryTextColor': '#fff', 'primaryBorderColor': '#4f46e5', 'lineColor': '#a5b4fc', 'actorBkg': '#6366f1', 'actorTextColor': '#fff', 'actorBorder': '#4f46e5', 'activationBkgColor': '#e0e7ff', 'activationBorderColor': '#6366f1', 'signalColor': '#4f46e5', 'noteBkgColor': '#fef3c7', 'noteBorderColor': '#f59e0b', 'noteTextColor': '#000', 'fontSize': '13px' }}}%%
sequenceDiagram
    autonumber

    actor User
    participant Gen as FlightDataGenerator
    participant Model as FlightRecord<br/>(Pydantic)
    participant Stream as json_stream
    participant Arrow as PyArrow
    participant Duck as DuckDB
    participant Ice as Iceberg Catalog

    User ->>+ Gen : generate(n=500, seed=42)
    Note over Gen : Deterministic RNG<br/>Faker + weighted status

    loop For each of n records
        Gen ->> Model : FlightRecord(**fields)
        Model -->> Gen : Validated record
    end
    Gen -->>- User : list[FlightRecord]

    User ->>+ Stream : stream_to_file(records, path)
    loop For each record
        Stream ->> Stream : model_dump_json()
        Stream ->> Stream : write line + "\n"
    end
    Stream -->>- User : count = 500

    User ->>+ Arrow : ndjson_file_to_arrow(path)
    Arrow ->> Arrow : paj.read_json()
    Arrow -->>- User : pa.Table (500 rows)

    User ->>+ Duck : arrow_to_duckdb(table, con)
    Duck ->> Duck : register("__arrow_flights", table)
    Duck ->> Duck : CREATE TABLE flights AS SELECT *
    Duck ->> Duck : unregister("__arrow_flights")
    Duck -->>- User : row_count = 500

    User ->>+ Ice : append_arrow_to_iceberg(catalog, table)
    Ice ->> Ice : create_flights_table() [idempotent]
    Ice ->> Ice : _coerce_arrow_schema()
    Ice ->> Ice : table.append(coerced)
    Note over Ice : New snapshot created<br/>Parquet files written
    Ice -->>- User : rows_appended = 500

    User ->> Duck : SELECT status, count(*) GROUP BY status
    Duck -->> User : Aggregated results

    User ->> Ice : scan_iceberg_table(filter="status == 'delayed'")
    Ice -->> User : Filtered pa.Table
```

---

## Class Diagram

The object model and relationships between all source modules.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#6366f1', 'primaryTextColor': '#fff', 'primaryBorderColor': '#4f46e5', 'lineColor': '#a5b4fc', 'classText': '#1e1b4b', 'fontSize': '12px' }}}%%
classDiagram
    direction TB

    namespace generators {
        class FlightStatus {
            <<enumeration>>
            SCHEDULED
            BOARDING
            DEPARTED
            IN_AIR
            LANDED
            ARRIVED
            DELAYED
            CANCELLED
            DIVERTED
        }

        class FlightType {
            <<enumeration>>
            ARRIVAL
            DEPARTURE
        }

        class FlightRecord {
            <<Pydantic BaseModel>>
            +str flight_id
            +FlightType flight_type
            +str airline
            +str airline_code
            +int flight_number
            +str origin_airport
            +str destination_airport
            +datetime scheduled_time
            +datetime? estimated_time
            +datetime? actual_time
            +FlightStatus status
            +str? gate
            +str? terminal
            +str? aircraft_type
            +int delay_minutes
            +uppercase_airport(v) str
            +uppercase_airline_code(v) str
        }

        class FlightDataGenerator {
            +int seed
            +Random rng
            +Faker faker
            +datetime base_time
            +generate(n: int) list~FlightRecord~
            +stream(n: int) Iterator~FlightRecord~
            -_make_record() FlightRecord
        }
    }

    namespace streaming {
        class JsonStream {
            <<module>>
            +record_to_json_line(record) str
            +records_to_ndjson(records) str
            +stream_to_file(records, path) int
            +stream_to_buffer(records) BytesIO
            +read_ndjson_file(path) list~dict~
            +count_lines(path) int
        }
    }

    namespace ingestion {
        class ArrowLoader {
            <<module>>
            +FLIGHT_ARROW_SCHEMA : pa.Schema
            +ndjson_file_to_arrow(path) pa.Table
            +ndjson_buffer_to_arrow(buf) pa.Table
            +arrow_to_duckdb(table, con, name, mode) int
            +load_ndjson_to_duckdb(path, con, name) int
            +get_duckdb_table_schema(con, name) list
            +query_flights(con, name, where, limit) pa.Table
        }
    }

    namespace catalog {
        class IcebergCatalog {
            <<module>>
            +FLIGHT_ICEBERG_SCHEMA : Schema
            +create_catalog(path, name) SqlCatalog
            +create_flights_table(catalog, ns, name) Table
            +append_arrow_to_iceberg(catalog, table) int
            +scan_iceberg_table(catalog, ns, name, filter) pa.Table
            +list_snapshots(catalog, ns, name) list~dict~
            +drop_warehouse(path) None
            -_coerce_arrow_schema(table) pa.Table
            -_warehouse_uri(path) str
        }
    }

    FlightRecord --> FlightStatus : status
    FlightRecord --> FlightType : flight_type
    FlightDataGenerator --> FlightRecord : creates
    JsonStream --> FlightRecord : serializes
    ArrowLoader --> JsonStream : reads NDJSON from
    IcebergCatalog --> ArrowLoader : receives pa.Table from
```

---

## Entity-Relationship Diagram

The logical data model as it exists across DuckDB tables and the Iceberg catalog.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#6366f1', 'primaryTextColor': '#fff', 'primaryBorderColor': '#4f46e5', 'lineColor': '#a5b4fc', 'fontSize': '12px' }}}%%
erDiagram
    AIRLINE {
        string airline_code PK "IATA 2-3 letter code"
        string airline_name "Full airline name"
    }

    AIRPORT {
        string airport_code PK "IATA 3-4 letter code"
    }

    AIRCRAFT {
        string aircraft_type PK "ICAO type designator"
    }

    FLIGHT {
        string flight_id PK "e.g. UA-1234"
        string flight_type "arrival | departure"
        string airline_code FK "References AIRLINE"
        int flight_number "1–9999"
        string origin_airport FK "References AIRPORT"
        string destination_airport FK "References AIRPORT"
        datetime scheduled_time "UTC ISO-8601"
        datetime estimated_time "nullable"
        datetime actual_time "nullable"
        string status "FlightStatus enum"
        string gate "nullable, e.g. B12"
        string terminal "nullable, e.g. T1"
        string aircraft_type FK "References AIRCRAFT"
        int delay_minutes "0 = on time"
    }

    ICEBERG_SNAPSHOT {
        long snapshot_id PK "Auto-generated"
        long timestamp_ms "Epoch milliseconds"
        string operation "append"
        string manifest_list "Path to manifests"
    }

    ICEBERG_CATALOG {
        string catalog_name PK "e.g. flight_catalog"
        string warehouse_uri "file:/// path"
        string namespace "e.g. flights_db"
    }

    AIRLINE ||--o{ FLIGHT : "operates"
    AIRPORT ||--o{ FLIGHT : "origin"
    AIRPORT ||--o{ FLIGHT : "destination"
    AIRCRAFT ||--o{ FLIGHT : "aircraft_type"
    ICEBERG_CATALOG ||--o{ FLIGHT : "catalogs"
    ICEBERG_CATALOG ||--o{ ICEBERG_SNAPSHOT : "tracks versions"
    ICEBERG_SNAPSHOT }o--|| FLIGHT : "snapshot of"
```

---

## Project Structure

```
flight-streaming-pipeline/
├── pyproject.toml                          # Build config, pytest markers
├── requirements.txt                        # Pinned dependencies
├── src/
│   ├── generators/
│   │   ├── models.py                       # FlightRecord, FlightStatus, FlightType
│   │   └── flight_generator.py             # DataDesigner-pattern generator
│   ├── streaming/
│   │   └── json_stream.py                  # NDJSON serialization & file/buffer IO
│   ├── ingestion/
│   │   └── arrow_loader.py                 # Arrow ↔ DuckDB zero-copy loader
│   └── catalog/
│       └── iceberg_catalog.py              # Iceberg catalog, table, snapshot mgmt
├── tests/
│   ├── conftest.py                         # Shared fixtures (generator, DuckDB, paths)
│   ├── feature_1_data_generation/          # 25 tests — schema, determinism, constraints
│   ├── feature_2_json_streaming/           # 20 tests — serialization, round-trip, edge cases
│   ├── feature_3_arrow_ingestion/          # 15 tests — Arrow load, DuckDB queries, fidelity
│   ├── feature_4_iceberg_catalog/          # 16 tests — catalog CRUD, snapshots, scans
│   └── feature_5_e2e_regression/           # 12 tests — full pipeline, determinism, integrity
└── data/
    ├── raw/                                # Generated NDJSON files (gitignored)
    └── iceberg/                            # Iceberg warehouse (gitignored)
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- pip

### Install

```bash
git clone https://github.com/JohnDinsdale43/flight-streaming-pipeline.git
cd flight-streaming-pipeline
pip install -r requirements.txt
```

### Quick Run

```python
from src.generators.flight_generator import FlightDataGenerator
from src.streaming.json_stream import stream_to_file
from src.ingestion.arrow_loader import load_ndjson_to_duckdb
from src.catalog.iceberg_catalog import create_catalog, append_arrow_to_iceberg
from src.ingestion.arrow_loader import ndjson_file_to_arrow
import duckdb

# 1. Generate 500 flights
gen = FlightDataGenerator(seed=42)
records = gen.generate(500)

# 2. Stream to NDJSON
stream_to_file(records, "data/raw/flights.ndjson")

# 3. Load into DuckDB via Arrow
con = duckdb.connect(":memory:")
load_ndjson_to_duckdb("data/raw/flights.ndjson", con, "flights")

# 4. Query
print(con.execute("SELECT status, count(*) FROM flights GROUP BY status").fetchall())

# 5. Catalog in Iceberg
catalog = create_catalog("data/iceberg")
arrow_table = ndjson_file_to_arrow("data/raw/flights.ndjson")
append_arrow_to_iceberg(catalog, arrow_table)
```

---

## Running Tests

```bash
# Full suite (88 tests)
pytest tests/ -v --cov=src

# By feature
pytest -m feature_1 -v          # Data generation (25 tests)
pytest -m feature_2 -v          # JSON streaming  (20 tests)
pytest -m feature_3 -v          # Arrow ingestion (15 tests)
pytest -m feature_4 -v          # Iceberg catalog (16 tests)
pytest -m feature_5 -v          # E2E regression  (12 tests)
```

---

## TDD Feature Breakdown

### Feature 1 — Data Generation (25 tests)

| ID | Test | Validates |
|----|------|-----------|
| 1.1 | Schema validation | Every record passes Pydantic validation |
| 1.2 | Determinism | Same seed → identical output |
| 1.3 | Batch generation | `generate(n)` returns exactly n records |
| 1.4 | Streaming generation | `stream(n)` yields exactly n records |
| 1.5 | Field constraints | Airport codes uppercase, flight_number in range |
| 1.6 | Status distribution | All 9 statuses appear given enough records |
| 1.7 | Delay logic | Delayed → delay > 0; Cancelled → delay = 0 |
| 1.8 | Airport pairs | Origin ≠ destination |
| 1.9 | Edge: zero | `generate(0)` → empty list |
| 1.10 | Edge: one | `generate(1)` → single valid record |

### Feature 2 — JSON Streaming (20 tests)

| ID | Test | Validates |
|----|------|-----------|
| 2.1 | Single record | Valid JSON, no embedded newlines |
| 2.2 | NDJSON string | Correct line count, each line parseable |
| 2.3 | File streaming | Writes correct number of lines |
| 2.4 | File round-trip | Write → read back preserves all fields |
| 2.5 | Buffer streaming | BytesIO contains valid NDJSON, position = 0 |
| 2.6 | Empty input | Produces empty string / file / buffer |
| 2.7 | Large batch | 1,000 records stream without error |
| 2.8 | Line counting | `count_lines()` matches written count |
| 2.9 | Datetime | ISO-8601 format preserved through serialization |
| 2.10 | Special chars | Airline names survive round-trip |

### Feature 3 — Arrow Ingestion (15 tests)

| ID | Test | Validates |
|----|------|-----------|
| 3.1 | File → Arrow | Correct row count and columns |
| 3.2 | Buffer → Arrow | Same fidelity as file path |
| 3.3 | Arrow → DuckDB create | Table exists with correct row count |
| 3.4 | Arrow → DuckDB append | Rows accumulate across appends |
| 3.5 | Schema inspection | DuckDB columns match expected schema |
| 3.6 | Filtered query | WHERE clause returns correct subset |
| 3.7 | Convenience load | Single-call NDJSON → DuckDB |
| 3.8 | Missing file | Raises `FileNotFoundError` |
| 3.9 | Empty file | Loads zero rows or raises gracefully |
| 3.10 | Data fidelity | Values survive NDJSON → Arrow → DuckDB |

### Feature 4 — Iceberg Catalog (16 tests)

| ID | Test | Validates |
|----|------|-----------|
| 4.1 | Catalog creation | SqlCatalog initializes, catalog.db exists |
| 4.2 | Table creation | Flights table has correct field names |
| 4.3 | Idempotent create | Calling twice returns same table handle |
| 4.4 | Append data | Row count matches after append |
| 4.5 | Multiple appends | Rows accumulate across appends |
| 4.6 | Full scan | Returns Arrow table with all rows |
| 4.7 | Filtered scan | Filter expression returns subset |
| 4.8 | Snapshot tracking | Each append creates a new snapshot |
| 4.9 | Warehouse cleanup | `drop_warehouse()` removes directory |
| 4.10 | Schema field count | Iceberg schema has exactly 15 fields |

### Feature 5 — End-to-End Regression (12 tests)

| ID | Test | Validates |
|----|------|-----------|
| 5.1 | Full pipeline | Generate → stream → ingest → catalog → query |
| 5.2 | Determinism | Same seed → identical DuckDB data across runs |
| 5.3 | Large dataset | 500 records end-to-end without error |
| 5.4 | Row count consistency | DuckDB and Iceberg counts match |
| 5.5 | DuckDB aggregation | GROUP BY, AVG queries return correct totals |
| 5.6 | Iceberg query | Scan returns all expected columns |
| 5.7 | Multiple runs | Iceberg accumulates, DuckDB replaces |
| 5.8 | Data integrity | Spot-check field values survive full pipeline |

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **DataDesigner pattern** | Schema-first generation with constraint pools (airports, airlines, status weights). Swappable to LLM-backed DataDesigner when endpoint available. |
| **NDJSON format** | Each line is an independent JSON object — ideal for incremental Arrow parsing and streaming ingestion. |
| **Arrow zero-copy** | DuckDB's `register()` creates virtual views over Arrow memory — no serialization overhead. |
| **FsspecFileIO** | Solves PyIceberg's PyArrow local filesystem path resolution issues on Windows (`C:\` parsed as URI scheme). |
| **LongType + nullable** | Iceberg schema uses `LongType` (int64) and `required=False` to match Arrow's default nullable int64 from JSON parsing. |
| **Deterministic seeds** | Every generator accepts a `seed` parameter — tests are fully reproducible across platforms. |

---

## License

MIT
